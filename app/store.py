"""调拨决定的持久化与两阶段收尾逻辑。

职责划分：

- 决定先写控制库（``control``），提交成功后才去碰两个业务库里的预备事务；
- 每条预备事务在准备阶段就写入一行``transfer_journal``（属于预备事务本身），
  因此收尾后可以从“预备事务列表里是否还有它”和“journal 行是否存在”
  唯一地判定它是被提交还是被撤销；
- 所有状态都是每次请求实时查询数据库得出的，进程内不保存任何结论。

数据库角色：

- ``control``：调拨登记、最终决定、收尾审计（权威决定来源）；
- ``source``：扣减库存的预备事务；
- ``target``：入库库存的预备事务。
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Callable, Literal, TypeVar
from urllib.parse import quote

import psycopg

Decision = Literal["commit", "rollback"]

T = TypeVar("T")

SIDE_SOURCE = "source"
SIDE_TARGET = "target"
SIDES = (SIDE_SOURCE, SIDE_TARGET)

# 观察到的状态
STATE_PREPARED = "prepared"        # 预备事务仍在
STATE_COMMITTED = "committed"      # 已提交（journal 有痕）
STATE_ROLLED_BACK = "rolled_back"  # 已撤销（journal 无痕）
STATE_UNKNOWN = "unknown"          # 登记不完整 / 证据矛盾
STATE_UNREACHABLE = "unreachable"  # 数据库连不上
STATE_TIMEOUT = "timeout"          # 等锁或执行超过 statement_timeout
STATE_ERROR = "error"              # 查询或收尾语句报错

EXPECTED_STATE = {
    "commit": STATE_COMMITTED,
    "rollback": STATE_ROLLED_BACK,
}


class StoreError(Exception):
    """存储层错误的基类。"""


class TransferNotFound(StoreError):
    def __init__(self, transfer_id: str):
        super().__init__(f"未知调拨号: {transfer_id}")
        self.transfer_id = transfer_id


class DecisionConflict(StoreError):
    def __init__(self, transfer_id: str, existing: str, requested: str):
        super().__init__(
            f"调拨 {transfer_id} 已登记决定 {existing}，不能再改成 {requested}")
        self.transfer_id = transfer_id
        self.existing = existing
        self.requested = requested


class ControlUnavailable(StoreError):
    """控制库不可用（此时决定没有持久化）。"""


@dataclass(frozen=True)
class DbEndpoint:
    key: str
    dsn: str | dict
    connect_timeout: int = 3
    statement_timeout: float = 5.0
    lock_timeout: float = 3.0

    def connect(self, *, autocommit: bool = True) -> psycopg.Connection:
        return psycopg.connect(
            self.dsn,
            autocommit=autocommit,
            connect_timeout=self.connect_timeout,
        )

    def run(self, fn: Callable[[psycopg.Connection], T],
            *, autocommit: bool = True, attempts: int = 2) -> T:
        """在一个连接上执行 fn，并设置语句超时。

        设置 statement_timeout，避免被另一侧尚未结束的预备事务持有行锁
        时无限等待——值班场景下必须给出确定的“未确认”，而不是挂住。
        连接级故障（连接断开等）重连重试一次；语句级错误与超时不重试。
        """
        timeout_ms = int(self.statement_timeout * 1000)
        lock_ms = int(self.lock_timeout * 1000)
        last: Exception | None = None
        for _ in range(max(1, attempts)):
            try:
                with self.connect(autocommit=autocommit) as conn:
                    # SET 不支持参数占位符，用 set_config（第三个参数 false
                    # 表示会话级）。
                    conn.execute(
                        "SELECT set_config('statement_timeout', %s, false), "
                        "       set_config('lock_timeout', %s, false)",
                        (f"{timeout_ms}ms", f"{lock_ms}ms"))
                    return fn(conn)
            except psycopg.OperationalError as exc:
                last = exc
                if isinstance(exc, (psycopg.errors.QueryCanceled,
                                    psycopg.errors.LockNotAvailable)):
                    raise
        assert last is not None
        raise last


def _endpoint(prefix: str, default_db: str) -> DbEndpoint:
    """优先使用 <PREFIX>_URL；否则由 HOST/PORT/DB 与 PGUSER/PGPASSWORD 拼装。"""
    timeout = int(os.environ.get(f"{prefix}_CONNECT_TIMEOUT", "3"))
    stmt_timeout = float(os.environ.get(f"{prefix}_STATEMENT_TIMEOUT", "5"))
    lock_timeout = float(os.environ.get(f"{prefix}_LOCK_TIMEOUT", "3"))
    url = os.environ.get(f"{prefix}_URL")
    if url:
        return DbEndpoint(prefix.lower(), url, timeout, stmt_timeout,
                          lock_timeout)
    host = os.environ.get(f"{prefix}_HOST", "localhost")
    port = int(os.environ.get(f"{prefix}_PORT", "5432"))
    dbname = os.environ.get(f"{prefix}_DB", default_db)
    user = quote(os.environ.get("PGUSER", "app"), safe="")
    password = quote(os.environ.get("PGPASSWORD", "app"), safe="")
    dsn = (f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
           f"?connect_timeout={timeout}")
    return DbEndpoint(prefix.lower(), dsn, timeout, stmt_timeout, lock_timeout)


CONTROL = _endpoint("CONTROL", "control")
SOURCE = _endpoint("SOURCE", "source")
TARGET = _endpoint("TARGET", "target")


@dataclass(frozen=True)
class Transfer:
    transfer_id: str
    source_gid: str
    target_gid: str
    sku: str
    qty: int
    note: str | None
    decision: str | None
    decided_at: datetime | None
    created_at: datetime | None

    def gid_for(self, side: str) -> str:
        if side == SIDE_SOURCE:
            return self.source_gid
        if side == SIDE_TARGET:
            return self.target_gid
        raise ValueError(side)


@dataclass
class SideState:
    side: str
    database: str
    gid: str
    state: str
    detail: str | None = None
    matches_decision: bool | None = None


# ---------------------------------------------------------------- 控制库


def _row_to_transfer(row) -> Transfer:
    return Transfer(
        transfer_id=row[0],
        source_gid=row[1],
        target_gid=row[2],
        sku=row[3],
        qty=row[4],
        note=row[5],
        decision=row[6],
        decided_at=row[7],
        created_at=row[8],
    )


_TRANSFER_COLUMNS = (
    "transfer_id, source_gid, target_gid, sku, qty, note, "
    "decision, decided_at, created_at"
)


def _fetch_transfer_conn(conn: psycopg.Connection, transfer_id: str) -> Transfer:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {_TRANSFER_COLUMNS} FROM transfers WHERE transfer_id = %s",
            (transfer_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise TransferNotFound(transfer_id)
    return _row_to_transfer(row)


def get_transfer(transfer_id: str) -> Transfer:
    """读取调拨登记；控制库不可用时抛 ControlUnavailable。"""
    try:
        return CONTROL.run(lambda conn: _fetch_transfer_conn(conn, transfer_id))
    except TransferNotFound:
        raise
    except psycopg.Error as exc:
        raise ControlUnavailable(f"控制库不可用: {exc}") from exc


def list_transfers() -> list[Transfer]:
    def run(conn: psycopg.Connection) -> list[Transfer]:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {_TRANSFER_COLUMNS} FROM transfers "
                "ORDER BY transfer_id")
            return [_row_to_transfer(row) for row in cur.fetchall()]

    try:
        return CONTROL.run(run)
    except psycopg.Error as exc:
        raise ControlUnavailable(f"控制库不可用: {exc}") from exc


def _log_decision(
    transfer_id: str, decision: str, outcome: str, detail: str | None = None
) -> None:
    """审计日志；尽力而为，不因日志失败而改变业务结论。"""
    try:
        CONTROL.run(lambda conn: conn.execute(
            "INSERT INTO decision_log (transfer_id, decision, outcome, detail) "
            "VALUES (%s, %s, %s, %s)",
            (transfer_id, decision, outcome, detail),
        ))
    except Exception:  # noqa: BLE001 - 审计是旁路
        pass


def record_decision(transfer_id: str, decision: Decision) -> tuple[Transfer, bool]:
    """先持久化决定，再返回。

    返回 (transfer, repeated)。重复提交同一决定时 repeated=True，调用方
    应继续处理；相反决定抛 DecisionConflict。本函数成功返回时，决定
    已经在控制库提交，即使随后进程立刻崩溃也不会丢失。
    """
    conflict: str | None = None

    def run(conn: psycopg.Connection) -> tuple[Transfer, bool] | None:
        nonlocal conflict
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {_TRANSFER_COLUMNS} FROM transfers "
                "WHERE transfer_id = %s FOR UPDATE",
                (transfer_id,),
            )
            row = cur.fetchone()
            if row is None:
                conn.rollback()
                raise TransferNotFound(transfer_id)
            transfer = _row_to_transfer(row)

            if transfer.decision is None:
                cur.execute(
                    "UPDATE transfers SET decision = %s, decided_at = now() "
                    "WHERE transfer_id = %s",
                    (decision, transfer_id),
                )
                # 重新读一次，拿到数据库生成的时间戳
                cur.execute(
                    f"SELECT {_TRANSFER_COLUMNS} FROM transfers "
                    "WHERE transfer_id = %s",
                    (transfer_id,),
                )
                transfer = _row_to_transfer(cur.fetchone())
                cur.execute(
                    "INSERT INTO decision_log "
                    "(transfer_id, decision, outcome) "
                    "VALUES (%s, %s, 'accepted')",
                    (transfer_id, decision),
                )
                conn.commit()
                return transfer, False

            if transfer.decision == decision:
                cur.execute(
                    "INSERT INTO decision_log "
                    "(transfer_id, decision, outcome) "
                    "VALUES (%s, %s, 'repeated')",
                    (transfer_id, decision),
                )
                conn.commit()
                return transfer, True

            # 相反决定：回滚（什么也没改），交给下面统一报冲突
            conflict = transfer.decision
            conn.rollback()
            return None

    try:
        result = CONTROL.run(run, autocommit=False)
    except psycopg.Error as exc:
        raise ControlUnavailable(f"控制库不可用: {exc}") from exc

    if result is not None:
        return result

    assert conflict is not None
    _log_decision(transfer_id, decision, "conflict", f"existing={conflict}")
    raise DecisionConflict(transfer_id, conflict, decision)


def record_side_results(transfer_id: str, states: list[SideState]) -> None:
    """把最近一次收尾观察写入控制库，供审计查看。失败不影响结论。"""
    def run(conn: psycopg.Connection) -> None:
        for st in states:
            conn.execute(
                """
                INSERT INTO side_results
                    (transfer_id, side, observed_state, matches_decision,
                     checked_at)
                VALUES (%s, %s, %s, %s, now())
                ON CONFLICT (transfer_id, side) DO UPDATE SET
                    observed_state = EXCLUDED.observed_state,
                    matches_decision = EXCLUDED.matches_decision,
                    checked_at = EXCLUDED.checked_at
                """,
                (transfer_id, st.side, st.state, st.matches_decision),
            )

    try:
        CONTROL.run(run)
    except Exception:  # noqa: BLE001 - 审计是旁路
        pass


# ---------------------------------------------------------------- 两库观察 / 收尾


def _short(exc: BaseException, limit: int = 300) -> str:
    text = f"{type(exc).__name__}: {exc}"
    return text if len(text) <= limit else text[:limit] + "..."


def _missing_state(endpoint: DbEndpoint, side: str, gid: str,
                   exc: BaseException) -> SideState:
    # 只有“连不上/连接中断”才算不可达；等锁超时另有专门状态，
    # 提示值班员“有别的会话占着锁”而不是“数据库坏了”。
    connect_failed = isinstance(exc, psycopg.OperationalError) and not isinstance(
        exc, (psycopg.errors.QueryCanceled, psycopg.errors.LockNotAvailable))
    if connect_failed:
        return SideState(side, endpoint.key, gid, STATE_UNREACHABLE, _short(exc))
    if isinstance(exc, (psycopg.errors.QueryCanceled,
                        psycopg.errors.LockNotAvailable)):
        return SideState(
            side, endpoint.key, gid, STATE_TIMEOUT,
            "语句超过 statement_timeout（可能有别的会话正持有该记录的行锁）："
            + _short(exc))
    return SideState(side, endpoint.key, gid, STATE_ERROR, _short(exc))


def _observe_side_conn(
    conn: psycopg.Connection, endpoint: DbEndpoint, transfer: Transfer, side: str
) -> SideState:
    """在给定连接上观察一侧的状态。

    判定顺序（越靠前越权威）：
    1. ``pg_prepared_xacts`` 里还有该 gid -> prepared；
    2. journal 行存在 -> committed（journal 随预备事务一起提交才可见，
       撤销不会留下行，因此它是“确实提交过”的持久证据，不依赖
       预备事务仍留在列表里）；
    3. 登记状态已是 prepared（准备阶段在独立连接里写入），而预备事务
       与 journal 都不在 -> rolled_back；
    4. 其余（没有登记、或登记还在 preparing）-> unknown。
    """
    gid = transfer.gid_for(side)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_prepared_xacts "
            "WHERE gid = %s AND database = current_database())",
            (gid,),
        )
        if cur.fetchone()[0]:
            return SideState(
                side, endpoint.key, gid, STATE_PREPARED,
                "预备事务仍在 pg_prepared_xacts")

        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM transfer_journal "
            "WHERE gid = %s AND transfer_id = %s AND side = %s)",
            (gid, transfer.transfer_id, side),
        )
        if cur.fetchone()[0]:
            return SideState(
                side, endpoint.key, gid, STATE_COMMITTED,
                "预备事务已不在列表，journal 行存在：确认已提交")

        cur.execute(
            "SELECT status FROM transfer_registry "
            "WHERE transfer_id = %s AND side = %s AND gid = %s",
            (transfer.transfer_id, side, gid),
        )
        row = cur.fetchone()
        if row is None:
            return SideState(
                side, endpoint.key, gid, STATE_UNKNOWN,
                "无登记、无 journal，且预备事务不在：无法判定")
        if row[0] != "prepared":
            return SideState(
                side, endpoint.key, gid, STATE_UNKNOWN,
                "登记状态仍为 preparing，且预备事务已不在：无法确认准备是否完成")
        return SideState(
            side, endpoint.key, gid, STATE_ROLLED_BACK,
            "预备事务已不在列表，journal 行不存在：确认已撤销")


def observe_side(endpoint: DbEndpoint, transfer: Transfer, side: str) -> SideState:
    gid = transfer.gid_for(side)
    try:
        return endpoint.run(
            lambda conn: _observe_side_conn(conn, endpoint, transfer, side))
    except psycopg.Error as exc:
        return _missing_state(endpoint, side, gid, exc)


def annotate(state: SideState, decision: str | None) -> SideState:
    """标出该侧结局是否与最终决定一致；未定论时为 None。"""
    if decision is None:
        return replace(state, matches_decision=None)
    expected = EXPECTED_STATE[decision]
    if state.state == expected:
        return replace(state, matches_decision=True)
    if state.state in (STATE_COMMITTED, STATE_ROLLED_BACK):
        return replace(state, matches_decision=False)
    return replace(state, matches_decision=None)


def observe_all(transfer: Transfer) -> list[SideState]:
    """并行观察两库（各自独立连接，单边不可达不拖住另一边）。"""
    with ThreadPoolExecutor(max_workers=len(SIDES)) as pool:
        futures = [
            (side, pool.submit(
                observe_side, SOURCE if side == SIDE_SOURCE else TARGET,
                transfer, side))
            for side in SIDES
        ]
        return [annotate(f.result(), transfer.decision) for _, f in futures]


def resolve_side(
    endpoint: DbEndpoint, transfer: Transfer, side: str, decision: Decision
) -> SideState:
    """按决定收尾一侧，并返回收尾后的真实状态。

    幂等：
    - 预备事务还在 -> 执行 COMMIT PREPARED / ROLLBACK PREPARED；
    - 预备事务已不在 -> 只做证据核对（journal 行）；
    - 并发下别人先一步收尾（42704）-> 重新观察，不当作失败。
    """
    gid = transfer.gid_for(side)

    def run(conn: psycopg.Connection) -> SideState:
        state = _observe_side_conn(conn, endpoint, transfer, side)
        if state.state == STATE_PREPARED:
            if decision == "commit":
                stmt = "COMMIT PREPARED {}".format(_quote_literal(gid))
            else:
                stmt = "ROLLBACK PREPARED {}".format(_quote_literal(gid))
            try:
                conn.execute(stmt)
            except psycopg.errors.UndefinedObject:
                # 另一个请求已经收尾；下面用证据重新判定
                pass
            state = _observe_side_conn(conn, endpoint, transfer, side)
        return annotate(state, decision)

    try:
        return endpoint.run(run)
    except psycopg.Error as exc:
        return annotate(_missing_state(endpoint, side, gid, exc), decision)


def _quote_literal(value: str) -> str:
    """PREPARE/COMMIT PREPARED 不支持参数占位符，按 SQL 字面量转义。"""
    return "'" + value.replace("'", "''") + "'"


def process_decision(transfer: Transfer, decision: Decision) -> list[SideState]:
    """按决定依次收尾两库；先扣减侧，后入库侧。"""
    states = [
        resolve_side(SOURCE, transfer, SIDE_SOURCE, decision),
        resolve_side(TARGET, transfer, SIDE_TARGET, decision),
    ]
    record_side_results(transfer.transfer_id, states)
    return states


def completed(states: list[SideState]) -> bool:
    return bool(states) and all(st.matches_decision is True for st in states)


def conflicting(states: list[SideState]) -> bool:
    return any(st.matches_decision is False for st in states)
