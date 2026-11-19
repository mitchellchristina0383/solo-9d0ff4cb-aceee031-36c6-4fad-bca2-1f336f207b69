"""准备跨仓调拨样例：两侧各留一个已登记的预备事务。

对每个调拨号 T：
1. 分别在扣减库、入库库的业务事务里改库存、写 ``transfer_journal``，
   然后 ``PREPARE TRANSACTION 'T:source'`` / ``'T:target'``；
2. 在独立连接（不在预备事务里）核实预备事务确实存在，并把
   ``transfer_registry.status`` 置为 ``prepared``；
3. 最后在控制库登记调拨号与两个 GID（也可以预置最终决定）。

为什么这样设计：

- ``transfer_journal`` 行只在预备事务**提交后**才可见，是“确实提交过”
  的持久证据；撤销不会留下它。收尾后即使预备事务已不在
  ``pg_prepared_xacts`` 里，也能据此区分 committed / rolled_back。
- ``transfer_registry`` 在预备事务之外维护，所以“这一侧准备完成了”
  这一事实本身不依赖预备事务的去留。
- 控制库登记最后写入：中途失败只会留下可撤销的预备事务，不会留下
  “已登记但没准备好”的调拨。

注意（2PC 固有限制）：预备事务在提交/撤销前一直持有它改过的行锁。
因此不要同时对同一个 SKU 准备两笔调拨，否则后一笔会等锁；本脚本用
``lock_timeout`` 快速失败并提示，而不是无限等待。

用法（在 api 容器或任意能连三库的环境里）：
    docker compose exec api python seed.py --transfer T-2024-0001
    docker compose exec api python seed.py --transfer T-2024-0002 --decision commit
    docker compose exec api python seed.py --transfer T-2024-0001 --force
"""

from __future__ import annotations

import argparse
import sys
from contextlib import contextmanager
from pathlib import Path

import psycopg

# 既是 `python seed.py` 脚本，也随 api 镜像一起分发；app/ 就在本文件旁边。
sys.path.insert(0, str(Path(__file__).resolve().parent))
import store  # noqa: E402
from store import CONTROL, SIDE_SOURCE, SIDE_TARGET, SOURCE, TARGET  # noqa: E402

LOCK_TIMEOUT_MS = 4000


@contextmanager
def _conn(endpoint, *, autocommit: bool):
    """普通连接，带锁等待上限，避免被另一笔未收尾的预备事务挂住。

    与 psycopg 的连接上下文管理器一致：正常退出时提交，抛异常时回滚。
    （psycopg 只对「连接自身作为上下文管理器」做这件事，这里手动管理，
    所以要显式补上。）
    """
    conn = endpoint.connect(autocommit=autocommit)
    try:
        conn.execute(
            "SELECT set_config('lock_timeout', %s, false), "
            "       set_config('statement_timeout', '15s', false)",
            (f"{LOCK_TIMEOUT_MS}ms",))
        yield conn
        if not autocommit:
            conn.commit()
    except BaseException:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


def _rollback_if_prepared(conn: psycopg.Connection, gid: str) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_prepared_xacts WHERE gid = %s", (gid,))
        if cur.fetchone() is not None:
            # 用独立语句执行（连接是 autocommit），不能在事务块里跑
            conn.execute("ROLLBACK PREPARED " + store._quote_literal(gid))


def _cleanup_leftovers(transfer_id: str) -> None:
    """撤销本调拨号残留的预备事务、登记与痕迹（可重复执行）。

    若该调拨此前已提交过，把库存改动按 journal 反向加回基线，避免样例
    反复重建时库存单调变化。
    """
    for endpoint, side in ((SOURCE, SIDE_SOURCE), (TARGET, SIDE_TARGET)):
        gid = f"{transfer_id}:{side}"
        try:
            # ROLLBACK PREPARED 不能在事务块里执行：清理/登记都用
            # autocommit 连接，每一步单独提交。
            with _conn(endpoint, autocommit=True) as conn:
                _rollback_if_prepared(conn, gid)
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT sku, delta FROM transfer_journal "
                        "WHERE gid = %s AND transfer_id = %s",
                        (gid, transfer_id),
                    )
                    row = cur.fetchone()
                    if row is not None:
                        cur.execute(
                            "UPDATE inventory SET on_hand = on_hand - %s "
                            "WHERE sku = %s", (row[1], row[0]))
                    cur.execute(
                        "DELETE FROM transfer_journal WHERE gid = %s "
                        "AND transfer_id = %s", (gid, transfer_id))
                    cur.execute(
                        "DELETE FROM transfer_registry "
                        "WHERE transfer_id = %s AND side = %s",
                        (transfer_id, side))
        except psycopg.errors.LockNotAvailable as exc:
            raise SystemExit(
                f"清理 {side} 失败：有其他会话正持有 {gid} 的行锁"
                f"（是不是另一笔未收尾的调拨用了同一个 SKU？）: {exc}") from exc
        except psycopg.Error as exc:
            raise SystemExit(f"清理 {side} 失败: {exc}") from exc

    try:
        with _conn(CONTROL, autocommit=True) as conn:
            conn.execute("DELETE FROM side_results WHERE transfer_id = %s",
                         (transfer_id,))
            conn.execute("DELETE FROM decision_log WHERE transfer_id = %s",
                         (transfer_id,))
            conn.execute("DELETE FROM transfers WHERE transfer_id = %s",
                         (transfer_id,))
    except psycopg.Error as exc:
        raise SystemExit(f"清理控制库登记失败: {exc}") from exc


def _ensure_sku(endpoint, side: str, sku: str, initial: int) -> None:
    """确保该 SKU 在库中存在（不存在则以 initial 建基线）。"""
    try:
        with _conn(endpoint, autocommit=True) as conn:
            conn.execute(
                "INSERT INTO inventory (sku, on_hand) VALUES (%s, %s) "
                "ON CONFLICT (sku) DO NOTHING", (sku, initial))
    except psycopg.Error as exc:
        raise SystemExit(f"{side}: 初始化 SKU {sku} 失败: {exc}") from exc


def _prepare_side(endpoint, side: str, transfer_id: str, sku: str,
                  delta: int) -> str:
    gid = f"{transfer_id}:{side}"

    # 1) 业务事务：改库存 + 写 journal，然后 PREPARE。
    try:
        with _conn(endpoint, autocommit=False) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE inventory SET on_hand = on_hand + %s WHERE sku = %s",
                    (delta, sku),
                )
                if cur.rowcount != 1:
                    raise SystemExit(
                        f"{side}: 库存表里没有 {sku}；"
                        f"请先在 db/{side}/init.sql 里准备该 SKU")
                cur.execute(
                    "INSERT INTO transfer_journal "
                    "(gid, transfer_id, side, sku, delta) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (gid, transfer_id, side, sku, delta),
                )
                # PREPARE 不支持参数占位符
                cur.execute("PREPARE TRANSACTION " + store._quote_literal(gid))
    except psycopg.errors.LockNotAvailable as exc:
        raise SystemExit(
            f"准备 {side} 失败：库存行被锁住——同一 SKU 上可能还有另一笔"
            f"未收尾的调拨。请先处理那笔调拨，或换一个 SKU: {exc}") from exc
    except psycopg.Error as exc:
        raise SystemExit(f"准备 {side} 预备事务失败: {exc}") from exc

    # 2) 在独立连接里核实预备事务存在，并把登记置为 prepared。
    try:
        with _conn(endpoint, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_prepared_xacts WHERE gid = %s", (gid,))
            if cur.fetchone() is None:
                raise SystemExit(f"{side}: PREPARE 之后找不到 {gid}")
            cur.execute(
                "UPDATE transfer_registry SET status = 'prepared', "
                "prepared_at = now() "
                "WHERE transfer_id = %s AND side = %s AND gid = %s",
                (transfer_id, side, gid),
            )
    except psycopg.Error as exc:
        raise SystemExit(f"更新 {side} 准备状态失败: {exc}") from exc
    return gid


def _register_preparing(endpoint, side: str, transfer_id: str, gid: str) -> None:
    """准备事务之前先在独立连接里写下“准备中”的登记。"""
    try:
        with _conn(endpoint, autocommit=True) as conn:
            conn.execute(
                "INSERT INTO transfer_registry "
                "(transfer_id, side, gid, status, prepared_at) "
                "VALUES (%s, %s, %s, 'preparing', NULL) "
                "ON CONFLICT (transfer_id, side) DO UPDATE SET "
                "gid = EXCLUDED.gid, status = 'preparing', prepared_at = NULL",
                (transfer_id, side, gid),
            )
    except psycopg.Error as exc:
        raise SystemExit(f"登记 {side} 准备状态失败: {exc}") from exc


def _register_control(transfer_id: str, source_gid: str, target_gid: str,
                      sku: str, qty: int, note: str | None,
                      decision: str | None) -> None:
    try:
        with _conn(CONTROL, autocommit=False) as conn:
            if decision is None:
                conn.execute(
                    "INSERT INTO transfers "
                    "(transfer_id, source_gid, target_gid, sku, qty, note) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (transfer_id, source_gid, target_gid, sku, qty, note),
                )
            else:
                conn.execute(
                    "INSERT INTO transfers "
                    "(transfer_id, source_gid, target_gid, sku, qty, note, "
                    " decision, decided_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, now())",
                    (transfer_id, source_gid, target_gid, sku, qty, note,
                     decision),
                )
                conn.execute(
                    "INSERT INTO decision_log "
                    "(transfer_id, decision, outcome, detail) "
                    "VALUES (%s, %s, 'accepted', 'seeded with decision')",
                    (transfer_id, decision),
                )
    except psycopg.Error as exc:
        raise SystemExit(f"控制库登记失败: {exc}") from exc


def seed(transfer_id: str, sku: str, qty: int, note: str | None,
         decision: str | None) -> None:
    source_gid = f"{transfer_id}:{SIDE_SOURCE}"
    target_gid = f"{transfer_id}:{SIDE_TARGET}"
    # 库存行不存在时自动建基线，便于给任意新 SKU 准备样例。
    _ensure_sku(SOURCE, SIDE_SOURCE, sku, 1000)
    _ensure_sku(TARGET, SIDE_TARGET, sku, 200)
    _register_preparing(SOURCE, SIDE_SOURCE, transfer_id, source_gid)
    _register_preparing(TARGET, SIDE_TARGET, transfer_id, target_gid)

    source_gid = _prepare_side(SOURCE, SIDE_SOURCE, transfer_id, sku, -qty)
    try:
        target_gid = _prepare_side(TARGET, SIDE_TARGET, transfer_id, sku, qty)
    except BaseException:
        # 入库侧没准备好：把扣减侧撤掉，不留单边
        with _conn(SOURCE, autocommit=True) as conn:
            conn.execute("ROLLBACK PREPARED "
                         + store._quote_literal(source_gid))
        raise
    _register_control(transfer_id, source_gid, target_gid, sku, qty, note,
                      decision)
    print(f"seeded {transfer_id}: sku={sku} qty={qty} "
          f"gids={source_gid!r},{target_gid!r} "
          f"decision={decision or '(未决定)'}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="准备跨仓调拨样例（两侧各一个预备事务）")
    parser.add_argument("--transfer", default="T-2024-0001")
    parser.add_argument("--sku", default="SKU-1001")
    parser.add_argument("--qty", type=int, default=10)
    parser.add_argument("--note", default="样例调拨")
    parser.add_argument("--decision", choices=["commit", "rollback"],
                        default=None,
                        help="预置最终决定（用于演示“决定已定、尚未收尾”）")
    parser.add_argument("--force", action="store_true",
                        help="先撤销遗留预备事务、删除登记与痕迹后重建")
    args = parser.parse_args(argv)

    if args.qty <= 0:
        raise SystemExit("qty 必须为正数")

    if args.force:
        _cleanup_leftovers(args.transfer)
    else:
        try:
            store.get_transfer(args.transfer)
        except store.TransferNotFound:
            pass
        except store.ControlUnavailable as exc:
            raise SystemExit(str(exc)) from exc
        else:
            raise SystemExit(f"{args.transfer} 已登记；如需重建请加 --force")

    seed(args.transfer, args.sku, args.qty, args.note, args.decision)
    return 0


if __name__ == "__main__":
    sys.exit(main())
