"""跨仓调拨决定 API。

两名数据库值班员分别负责扣减库（source）与入库库（target）。两边的
预备事务都已 PREPARE，等待一个最终决定。本服务：

1. 先把决定持久化到控制库（control），提交成功后才去碰业务库；
2. 再按决定对两侧的预备事务执行 COMMIT PREPARED / ROLLBACK PREPARED；
3. 通过“预备事务是否还在”+“journal 行是否存在”确认每一侧的最终结局，
   两库都确认与决定一致后才报告完成。

所有结论都来自实时查询，进程重启不会丢失进度；重复提交同一决定会
继续处理，相反决定返回冲突。
"""

from __future__ import annotations

import logging

import psycopg
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, Field

import store
from store import (
    CONTROL,
    ControlUnavailable,
    DecisionConflict,
    SideState,
    Transfer,
    TransferNotFound,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("transfer-api")


class DecisionBody(BaseModel):
    decision: str = Field(..., pattern="^(commit|rollback)$",
                          description="commit=提交（两边都生效）；"
                                      "rollback=撤销（两边都回滚）")


class SideView(BaseModel):
    side: str = Field(description="source=扣减库，target=入库库")
    database: str
    gid: str
    state: str = Field(description="prepared / committed / rolled_back / "
                                   "unreachable / timeout / unknown / error")
    matches_decision: bool | None = Field(
        description="true=已按最终决定结束；false=结局与决定矛盾；"
                    "null=尚未定论")
    detail: str | None = None


class Summary(BaseModel):
    transfer_id: str
    sku: str
    qty: int
    note: str | None
    decision: str | None
    decided_at: str | None
    status: str = Field(description="undecided / pending / completed / "
                                    "needs_attention")
    completed: bool
    hint: str
    sides: list[SideView]
    repeated: bool | None = None
    pending_sides: list[str] | None = None
    conflicting_sides: list[str] | None = None


app = FastAPI(
    title="跨仓调拨决定 API",
    description=(
        "一笔跨仓调拨的扣减与入库分别以预备事务留在两个 PostgreSQL 库中。"
        "在这里持久化最终决定（提交 / 撤销）并收尾两侧，然后确认两边都已"
        "按决定结束。\n\n"
        "**流程**：`GET /transfers` 选调拨号 → `GET /transfers/{id}` 看两库"
        "状态 → `POST /transfers/{id}/decision` 提交决定。重复提交同一决定"
        "会继续处理；相反决定返回 409。"
    ),
    lifespan=None,
)


@app.exception_handler(TransferNotFound)
async def _on_not_found(req: Request, exc: TransferNotFound):
    return JSONResponse(
        {"error": "not_found", "detail": str(exc)}, status_code=404)


@app.exception_handler(DecisionConflict)
async def _on_conflict(req: Request, exc: DecisionConflict):
    return JSONResponse(
        {"error": "decision_conflict",
         "detail": str(exc),
         "existing_decision": exc.existing,
         "requested_decision": exc.requested},
        status_code=409)


@app.exception_handler(ControlUnavailable)
async def _on_control_unavailable(req: Request, exc: ControlUnavailable):
    # 决定没有被持久化，什么都还没改
    return JSONResponse(
        {"error": "control_unavailable", "detail": str(exc)}, status_code=503)


@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse("/docs")


def _side_dict(st: SideState) -> dict:
    return {
        "side": st.side,
        "database": st.database,
        "gid": st.gid,
        "state": st.state,
        "matches_decision": st.matches_decision,
        "detail": st.detail,
    }


def _summary(transfer: Transfer, states: list[SideState]) -> dict:
    completed = store.completed(states)
    unresolved = [st.side for st in states if st.matches_decision is None]
    if transfer.decision is None:
        status = "undecided"
        hint = "尚未登记决定；在 POST /transfers/{id}/decision 提交 commit 或 rollback。"
    elif completed:
        status = "completed"
        hint = "两库都已按决定收尾，可以交班。"
    elif unresolved:
        status = "pending"
        hint = "还有一侧未确认（未收尾或数据库不可达），可稍后重复提交同一决定继续处理。"
    else:
        status = "needs_attention"
        hint = "某一侧的结局与最终决定不一致，请人工核对。"
    return {
        "transfer_id": transfer.transfer_id,
        "sku": transfer.sku,
        "qty": transfer.qty,
        "note": transfer.note,
        "decision": transfer.decision,
        "decided_at": transfer.decided_at.isoformat()
        if transfer.decided_at else None,
        "status": status,
        "completed": completed,
        "hint": hint,
        "sides": [_side_dict(st) for st in states],
    }


@app.get("/transfers", summary="列出登记过的调拨及其决定")
def list_transfers():
    transfers = store.list_transfers()
    return {
        "transfers": [
            {
                "transfer_id": t.transfer_id,
                "sku": t.sku,
                "qty": t.qty,
                "source_gid": t.source_gid,
                "target_gid": t.target_gid,
                "decision": t.decision,
                "decided_at": t.decided_at.isoformat()
                if t.decided_at else None,
            }
            for t in transfers
        ]
    }


@app.get("/transfers/{transfer_id}", summary="查看调拨决定与两库状态",
         response_model=Summary)
def get_transfer(transfer_id: str):
    """实时查询两库：预备事务是否还在、journal 痕迹是否在。

    决定一旦登记，即使服务重启、预备事务已不在列表中，也能据此确认
    每一侧是按 commit 还是 rollback 结束的。
    """
    transfer = store.get_transfer(transfer_id)
    return _summary(transfer, store.observe_all(transfer))


@app.post("/transfers/{transfer_id}/decision",
          summary="提交最终决定并收尾两侧",
          response_model=Summary,
          responses={409: {"description": "已登记相反决定，或某侧结局与决定矛盾"}})
def post_decision(transfer_id: str, body: DecisionBody):
    """先持久化决定，再处理两库的预备事务。

    - 首次提交：记录决定后按决定收尾两侧；
    - 重复提交同一决定：继续收尾（幂等，可反复调用）；
    - 相反决定：409，已登记的决定不变，两侧预备事务原样保留。
    """
    decision = body.decision
    transfer, repeated = store.record_decision(transfer_id, decision)
    logger.info("decision %s for %s (repeated=%s)", decision, transfer_id,
                repeated)

    states = store.process_decision(transfer, decision)
    summary = _summary(transfer, states)
    summary["repeated"] = repeated
    summary["pending_sides"] = [
        st.side for st in states if st.matches_decision is None]
    summary["conflicting_sides"] = [
        st.side for st in states if st.matches_decision is False]

    if store.conflicting(states):
        # 人已经做了决定，但数据库里的结局与之矛盾：不谎报完成
        return JSONResponse(summary, status_code=409)
    return summary


@app.get("/health", summary="控制库可用性")
def health():
    try:
        CONTROL.run(lambda conn: conn.execute("SELECT 1"), attempts=1)
        return {"status": "ok"}
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail=f"control: {exc}") from exc
