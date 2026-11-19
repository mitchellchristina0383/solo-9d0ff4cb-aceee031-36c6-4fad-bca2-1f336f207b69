-- 入库库：与扣减库同构（两个独立的 PostgreSQL 实例）。

CREATE TABLE IF NOT EXISTS inventory (
    sku        text PRIMARY KEY,
    on_hand    integer NOT NULL CHECK (on_hand >= 0)
);

CREATE TABLE IF NOT EXISTS transfer_registry (
    transfer_id  text NOT NULL,
    side         text NOT NULL CHECK (side IN ('source', 'target')),
    gid          text NOT NULL UNIQUE,
    status       text NOT NULL CHECK (status IN ('preparing', 'prepared')),
    prepared_at  timestamptz,
    PRIMARY KEY (transfer_id, side)
);

CREATE TABLE IF NOT EXISTS transfer_journal (
    gid          text PRIMARY KEY,
    transfer_id  text NOT NULL,
    side         text NOT NULL CHECK (side IN ('source', 'target')),
    sku          text NOT NULL,
    delta        integer NOT NULL,
    written_at   timestamptz NOT NULL DEFAULT now()
);

INSERT INTO inventory (sku, on_hand) VALUES ('SKU-1001', 20)
ON CONFLICT (sku) DO NOTHING;
INSERT INTO inventory (sku, on_hand) VALUES ('SKU-1002', 10)
ON CONFLICT (sku) DO NOTHING;

-- 与扣减库一致的自测专用 SKU。
INSERT INTO inventory (sku, on_hand)
SELECT 'SKU-90' || lpad(g::text, 2, '0'), 200
FROM generate_series(1, 20) AS g
ON CONFLICT (sku) DO NOTHING;
