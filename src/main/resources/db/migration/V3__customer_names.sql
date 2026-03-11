-- ── Customer identities (display names) ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS customer (
    customer_id  VARCHAR(64) PRIMARY KEY,
    first_name   VARCHAR(128) NOT NULL,
    middle_name  VARCHAR(128) NOT NULL,
    last_name    VARCHAR(128) NOT NULL,
    display_name VARCHAR(384) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_customer_display_name
    ON customer(display_name);
