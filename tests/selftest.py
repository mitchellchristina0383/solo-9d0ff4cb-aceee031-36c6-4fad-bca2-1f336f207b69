#!/usr/bin/env python3
"""跨仓调拨决定 API 的端到端自测。

用前先安装依赖：pip install -r tests/requirements.txt

覆盖：
- 未登记决定的调拨：两侧都应是 prepared，不能报完成；
- 提交：两库都按决定收尾，库存变化正确，重复提交幂等；
- 撤销：两库都回滚，库存不变；
- 相反决定：409，已登记决定不变；
- 未知调拨号：404；
- 决定已持久化、尚未收尾时进程重启：GET 仍能看到决定，重复提交可继续
  并最终报完成；
- 单边处理后进程重启（预备事务已不在列表里）：仍能确认该侧按决定
  结束，再处理另一侧；
- 并发提交同一决定：都成功，不重复扣减/入库。

前置条件：
- 三个 PostgreSQL 库已按 db/*/init.sql 初始化；
- API 已在 BASE_URL（默认 http://127.0.0.1:8000）运行；
- 环境变量 CONTROL_URL / SOURCE_URL / TARGET_URL 指向三个库；
- 可选 RESTART_CMD：用于模拟进程重启，如
  "docker compose restart api" 或一个本地脚本；未提供则跳过重启用例。

用法：
    python3 tests/selftest.py
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
import psycopg

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000")
CONTROL_URL = os.environ.get("CONTROL_URL")
SOURCE_URL = os.environ.get("SOURCE_URL")
TARGET_URL = os.environ.get("TARGET_URL")
RESTART_CMD = os.environ.get("RESTART_CMD")
TRANSFER = os.environ.get("TRANSFER_ID", "T-2024-0001")

failures: list[str] = []

# 每个用例用独立 SKU：预备事务在收尾前持有该行的写锁，共用 SKU 会让
# 后准备的那笔一直等锁；跨测试运行也必须唯一，否则上次运行残留的预备
# 事务会锁住同名行。seed.py 会在需要时自动建基线库存。
def next_sku() -> str:
    return f"SKU-T{uuid.uuid4().hex[:10]}"


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}"
          + (f"  -- {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(f"{name}: {detail}")


def wait_api(timeout: float = 40) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if httpx.get(f"{BASE}/health", timeout=2).status_code == 200:
                return
        except Exception:  # noqa: BLE001
            pass
        time.sleep(0.3)
    raise SystemExit(f"API not reachable at {BASE}")


def restart_api() -> None:
    if not RESTART_CMD:
        return
    subprocess.run(RESTART_CMD, shell=True, check=True,
                   stdout=subprocess.DEVNULL)
    wait_api()


def q(url: str, sql: str, params: tuple = ()) -> list[tuple]:
    with psycopg.connect(url, autocommit=True, connect_timeout=3) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description is None:
                return []
            return cur.fetchall()


def side_stock(url: str, sku: str) -> int:
    rows = q(url, "SELECT on_hand FROM inventory WHERE sku = %s", (sku,))
    return rows[0][0] if rows else -1


def prepared_gids(url: str) -> list[str]:
    return [r[0] for r in q(url, "SELECT gid FROM pg_prepared_xacts")]


def side_row(url: str, transfer_id: str, side: str):
    rows = q(url,
             "SELECT gid, status FROM transfer_registry "
             "WHERE transfer_id = %s AND side = %s", (transfer_id, side))
    return rows[0] if rows else None


def seed(transfer_id: str, sku: str, qty: int,
         decision: str | None = None) -> None:
    """调用随仓库提供的样例准备脚本（需要与 API 相同的三库环境变量）。

    在独立进程里跑，所以不会把测试进程里的连接带进新预备事务的锁冲突。
    """
    root = Path(__file__).resolve().parent.parent
    cmd = [sys.executable, str(root / "app" / "seed.py"),
           "--transfer", transfer_id, "--sku", sku, "--qty", str(qty),
           "--force"]
    if decision:
        cmd += ["--decision", decision]
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        filter(None, [str(root / "app"), os.environ.get("PYTHONPATH")]))
    env.setdefault("PYTHONWARNINGS", "ignore")
    subprocess.run(cmd, check=True, env=env, stdout=subprocess.DEVNULL)


def decide(client: httpx.Client, transfer_id: str, decision: str):
    return client.post(f"/transfers/{transfer_id}/decision",
                       json={"decision": decision})


def state_of(payload: dict) -> dict[str, str]:
    return {s["side"]: s["state"] for s in payload["sides"]}


TEST_PREFIXES = ("T-UNDECIDED-", "T-COMMIT-", "T-ROLLBACK-", "T-CONFLICT-",
                 "T-RESTART-", "T-ONESIDE-", "T-ONERB-", "T-RACE-", "T-LIST-")


def cleanup_test_leftovers() -> None:
    """撤销上次中断的测试留下的预备事务与登记（只碰测试前缀）。"""
    for url in (SOURCE_URL, TARGET_URL):
        rows = q(url, "SELECT gid, database FROM pg_prepared_xacts")
        for gid, _db in rows:
            if gid.startswith(TEST_PREFIXES):
                q(url, "ROLLBACK PREPARED %s" % _lit(gid))


# ---------------------------------------------------------------- 用例

def t_undecided(client):
    tid = f"T-UNDECIDED-{uuid.uuid4().hex[:6]}"
    seed(tid, next_sku(), 2)
    r = client.get(f"/transfers/{tid}")
    check("undecided: 200", r.status_code == 200, r.text)
    d = r.json()
    check("undecided: status", d["status"] == "undecided", str(d))
    check("undecided: not completed", d["completed"] is False, str(d))
    check("undecided: both prepared",
          state_of(d) == {"source": "prepared", "target": "prepared"},
          str(state_of(d)))
    check("undecided: gids are prepared in both DBs",
          f"{tid}:source" in prepared_gids(SOURCE_URL)
          and f"{tid}:target" in prepared_gids(TARGET_URL))


def t_commit(client):
    tid = f"T-COMMIT-{uuid.uuid4().hex[:6]}"
    sku = next_sku()
    seed(tid, sku, 7)
    s0, t0 = side_stock(SOURCE_URL, sku), side_stock(TARGET_URL, sku)

    r = decide(client, tid, "commit")
    check("commit: 200", r.status_code == 200, r.text)
    d = r.json()
    check("commit: completed", d["completed"] is True, str(d))
    check("commit: both committed",
          state_of(d) == {"source": "committed", "target": "committed"},
          str(state_of(d)))
    check("commit: repeated=False", d["repeated"] is False, str(d))
    check("commit: source stock -7",
          side_stock(SOURCE_URL, sku) == s0 - 7)
    check("commit: target stock +7",
          side_stock(TARGET_URL, sku) == t0 + 7)
    check("commit: no prepared txns left",
          f"{tid}:source" not in prepared_gids(SOURCE_URL)
          and f"{tid}:target" not in prepared_gids(TARGET_URL))

    r2 = decide(client, tid, "commit")
    check("commit: repeat 200", r2.status_code == 200, r2.text)
    d2 = r2.json()
    check("commit: repeat repeated=True", d2["repeated"] is True, str(d2))
    check("commit: repeat still completed", d2["completed"] is True, str(d2))
    check("commit: repeat does not double-apply",
          side_stock(SOURCE_URL, sku) == s0 - 7
          and side_stock(TARGET_URL, sku) == t0 + 7)

    r3 = decide(client, tid, "rollback")
    check("commit: opposite -> 409", r3.status_code == 409, r3.text)
    check("commit: decision unchanged",
          client.get(f"/transfers/{tid}").json()["decision"] == "commit")


def t_rollback(client):
    tid = f"T-ROLLBACK-{uuid.uuid4().hex[:6]}"
    sku = next_sku()
    seed(tid, sku, 3)
    s0, t0 = side_stock(SOURCE_URL, sku), side_stock(TARGET_URL, sku)
    r = decide(client, tid, "rollback")
    check("rollback: 200", r.status_code == 200, r.text)
    d = r.json()
    check("rollback: completed", d["completed"] is True, str(d))
    check("rollback: both rolled back",
          state_of(d) == {"source": "rolled_back", "target": "rolled_back"},
          str(state_of(d)))
    check("rollback: stock unchanged",
          side_stock(SOURCE_URL, sku) == s0
          and side_stock(TARGET_URL, sku) == t0)
    r2 = decide(client, tid, "commit")
    check("rollback: opposite -> 409", r2.status_code == 409, r2.text)


def t_unknown(client):
    r = client.get("/transfers/T-DOES-NOT-EXIST")
    check("unknown: GET 404", r.status_code == 404, r.text)
    r = decide(client, "T-DOES-NOT-EXIST", "commit")
    check("unknown: POST 404", r.status_code == 404, r.text)


def t_conflict_does_not_touch_prepared(client):
    """相反决定被拒绝时，两侧预备事务必须原样保留。"""
    tid = f"T-CONFLICT-{uuid.uuid4().hex[:6]}"
    seed(tid, next_sku(), 1, decision="commit")
    r = decide(client, tid, "rollback")
    check("conflict: 409", r.status_code == 409, r.text)
    check("conflict: source still prepared",
          f"{tid}:source" in prepared_gids(SOURCE_URL))
    check("conflict: target still prepared",
          f"{tid}:target" in prepared_gids(TARGET_URL))
    d = client.get(f"/transfers/{tid}").json()
    check("conflict: decision still commit", d["decision"] == "commit", str(d))
    # 收尾，避免留下残留
    r = decide(client, tid, "commit")
    check("conflict: cleanup commit ok", r.status_code == 200, r.text)


def t_restart_before_processing(client):
    """决定已持久化、两侧都还没收尾时重启：GET 仍应看到决定与两个 prepared。"""
    tid = f"T-RESTART-{uuid.uuid4().hex[:6]}"
    seed(tid, next_sku(), 2, decision="commit")
    restart_api()
    d = client.get(f"/transfers/{tid}").json()
    check("restart: decision survived", d["decision"] == "commit", str(d))
    check("restart: pending before processing", d["completed"] is False,
          str(d))
    check("restart: both prepared",
          state_of(d) == {"source": "prepared", "target": "prepared"},
          str(state_of(d)))
    r = decide(client, tid, "commit")
    check("restart: repeated submit completes", r.status_code == 200
          and r.json()["completed"] is True, r.text)
    check("restart: marked repeated", r.json()["repeated"] is True, r.text)


def t_restart_after_one_side(client):
    """一侧已被别人/上一进程处理完，重启后仍能确认其结局并继续另一侧。"""
    tid = f"T-ONESIDE-{uuid.uuid4().hex[:6]}"
    sku = next_sku()
    # 预置决定（等同“决定已持久化但还没处理”），不经过 API 收尾
    seed(tid, sku, 5, decision="commit")
    s0, t0 = side_stock(SOURCE_URL, sku), side_stock(TARGET_URL, sku)
    # 模拟“一边处理完后进程挂了”：手工只提交扣减侧，然后重启
    q(SOURCE_URL, "COMMIT PREPARED %s" % _lit(f"{tid}:source"))
    restart_api()

    d = client.get(f"/transfers/{tid}").json()
    states = state_of(d)
    check("oneside: source committed from evidence",
          states["source"] == "committed", str(d))
    check("oneside: target still prepared", states["target"] == "prepared",
          str(d))
    check("oneside: not completed yet", d["completed"] is False, str(d))

    r = decide(client, tid, "commit")
    check("oneside: completes after repeat", r.status_code == 200
          and r.json()["completed"] is True, r.text)
    check("oneside: both committed",
          state_of(r.json()) == {"source": "committed",
                                 "target": "committed"}, r.text)
    check("oneside: stock applied once",
          side_stock(SOURCE_URL, sku) == s0 - 5
          and side_stock(TARGET_URL, sku) == t0 + 5)


def t_restart_after_rollback_side(client):
    """撤销场景的单边处理：已撤销的一侧不再出现在预备列表，仍能确认。"""
    tid = f"T-ONERB-{uuid.uuid4().hex[:6]}"
    sku = next_sku()
    seed(tid, sku, 4, decision="rollback")
    s0, t0 = side_stock(SOURCE_URL, sku), side_stock(TARGET_URL, sku)
    # 模拟“撤销侧已处理完、进程挂了”
    q(SOURCE_URL, "ROLLBACK PREPARED %s" % _lit(f"{tid}:source"))
    restart_api()

    d = client.get(f"/transfers/{tid}").json()
    states = state_of(d)
    check("onerb: source rolled_back from evidence",
          states["source"] == "rolled_back", str(d))
    check("onerb: target still prepared", states["target"] == "prepared",
          str(d))
    r = decide(client, tid, "rollback")
    check("onerb: completes after repeat", r.status_code == 200
          and r.json()["completed"] is True, r.text)
    check("onerb: stock unchanged",
          side_stock(SOURCE_URL, sku) == s0
          and side_stock(TARGET_URL, sku) == t0)


def _lit(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def t_concurrent_same_decision(client):
    tid = f"T-RACE-{uuid.uuid4().hex[:6]}"
    sku = next_sku()
    seed(tid, sku, 2)
    s0, t0 = side_stock(SOURCE_URL, sku), side_stock(TARGET_URL, sku)

    async def run() -> list[httpx.Response]:
        async with httpx.AsyncClient(base_url=BASE, timeout=30) as ac:
            return await asyncio.gather(*[
                ac.post(f"/transfers/{tid}/decision",
                        json={"decision": "commit"})
                for _ in range(4)])

    results = asyncio.run(run())
    codes = [r.status_code for r in results]
    check("race: all 200", all(c == 200 for c in codes), str(codes))
    check("race: all completed",
          all(r.json().get("completed") for r in results if r.status_code == 200),
          str([r.text for r in results]))
    check("race: stock applied once",
          side_stock(SOURCE_URL, sku) == s0 - 2
          and side_stock(TARGET_URL, sku) == t0 + 2)


def t_list(client):
    tid = f"T-LIST-{uuid.uuid4().hex[:6]}"
    seed(tid, next_sku(), 1)
    r = client.get("/transfers")
    check("list: 200", r.status_code == 200, r.text)
    ids = [t["transfer_id"] for t in r.json()["transfers"]]
    check("list: contains seeded", tid in ids, str(ids[:5]))


def main() -> int:
    for name, url in (("CONTROL_URL", CONTROL_URL), ("SOURCE_URL", SOURCE_URL),
                      ("TARGET_URL", TARGET_URL)):
        if not url:
            print(f"请设置 {name}")
            return 2
    wait_api()
    cleanup_test_leftovers()
    print(f"API up at {BASE}; restart tests: "
          f"{'on' if RESTART_CMD else 'off (RESTART_CMD 未设置)'}")
    with httpx.Client(base_url=BASE, timeout=60) as client:
        t_undecided(client)
        t_commit(client)
        t_rollback(client)
        t_unknown(client)
        t_conflict_does_not_touch_prepared(client)
        t_list(client)
        t_concurrent_same_decision(client)
        if RESTART_CMD:
            t_restart_before_processing(client)
            t_restart_after_one_side(client)
            t_restart_after_rollback_side(client)
    cleanup_test_leftovers()
    print()
    if failures:
        print(f"{len(failures)} FAILURE(S):")
        for f in failures:
            print(" -", f)
        return 1
    print("ALL TESTS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
