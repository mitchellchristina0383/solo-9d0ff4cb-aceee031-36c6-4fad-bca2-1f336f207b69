-- 控制库：调拨登记、最终决定、审计。
-- 本库不存放任何预备事务；决定在这里持久化后，才去收尾两个业务库。

CREATE TABLE IF NOT EXISTS transfers (
    transfer_id   text PRIMARY KEY,
    source_gid    text NOT NULL UNIQUE,
    target_gid    text NOT NULL UNIQUE,
    sku           text NOT NULL,
    qty           integer NOT NULL CHECK (qty > 0),
    note          text,
    decision      text CHECK (decision IN ('commit', 'rollback')),
    decided_at    timestamptz,
    created_at    timestamptz NOT NULL DEFAULT now(),
    CHECK ((decision IS NULL) = (decided_at IS NULL))
);

-- 只有第一次登记决定会被写入；之后同一决定的重复提交只记审计。
CREATE TABLE IF NOT EXISTS decision_log (
    id          bigserial PRIMARY KEY,
    transfer_id text NOT NULL,
    decision    text NOT NULL,
    outcome     text NOT NULL,
    detail      text,
    logged_at   timestamptz NOT NULL DEFAULT now()
);

-- 最近一次收尾观察到的两库状态（审计用，不是判定依据）。
CREATE TABLE IF NOT EXISTS side_results (
    transfer_id     text NOT NULL,
    side            text NOT NULL CHECK (side IN ('source', 'target')),
    observed_state  text NOT NULL,
    matches_decision boolean,
    checked_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (transfer_id, side)
);
