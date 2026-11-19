-- 扣减库：调拨样例使用的库存表与调拨登记。

CREATE TABLE IF NOT EXISTS inventory (
    sku        text PRIMARY KEY,
    on_hand    integer NOT NULL CHECK (on_hand >= 0)
);

-- 登记一侧的预备事务。status=prepared 表示 PREPARE TRANSACTION 已完成；
-- 只有 status=prepared 的登记才允许据此判定“预备事务是否已被提交/撤销”。
CREATE TABLE IF NOT EXISTS transfer_registry (
    transfer_id  text NOT NULL,
    side         text NOT NULL CHECK (side IN ('source', 'target')),
    gid          text NOT NULL UNIQUE,
    status       text NOT NULL CHECK (status IN ('preparing', 'prepared')),
    prepared_at  timestamptz,
    PRIMARY KEY (transfer_id, side)
);

-- 随预备事务一起提交才会留下的“生效痕迹”。撤销的预备事务不会产生行，
-- 因此收尾后据此区分 committed / rolled_back，即使预备事务早已不在
-- pg_prepared_xacts 列表里（例如进程重启后）。
CREATE TABLE IF NOT EXISTS transfer_journal (
    gid          text PRIMARY KEY,
    transfer_id  text NOT NULL,
    side         text NOT NULL CHECK (side IN ('source', 'target')),
    sku          text NOT NULL,
    delta        integer NOT NULL,
    written_at   timestamptz NOT NULL DEFAULT now()
);

INSERT INTO inventory (sku, on_hand) VALUES ('SKU-1001', 100)
ON CONFLICT (sku) DO NOTHING;
INSERT INTO inventory (sku, on_hand) VALUES ('SKU-1002', 50)
ON CONFLICT (sku) DO NOTHING;

-- 自测专用 SKU：与演示样例分开，避免自测的预备事务与未收尾的样例
-- 争抢同一行锁（预备事务在收尾前一直持有它改过的行锁）。
INSERT INTO inventory (sku, on_hand)
SELECT 'SKU-90' || lpad(g::text, 2, '0'), 1000
FROM generate_series(1, 20) AS g
ON CONFLICT (sku) DO NOTHING;
