-- =============================================================================
-- Customer Similarity Engine — PostgreSQL schema
-- This script runs automatically on first container start via Flyway.
-- =============================================================================

-- ── Raw transactions ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS transaction (
    id               BIGSERIAL      PRIMARY KEY,
    customer_id      VARCHAR(64)    NOT NULL,
    category         VARCHAR(128)   NOT NULL,
    amount           DECIMAL(15,2)  NOT NULL,
    transaction_date DATE           NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_txn_customer ON transaction(customer_id);
CREATE INDEX IF NOT EXISTS idx_txn_date     ON transaction(transaction_date);

-- ── Monthly category scores (materialised) ───────────────────────────────────
CREATE TABLE IF NOT EXISTS monthly_category_score (
    id                  BIGSERIAL        PRIMARY KEY,
    customer_id         VARCHAR(64)      NOT NULL,
    category            VARCHAR(128)     NOT NULL,
    year_month          VARCHAR(7)       NOT NULL,
    raw_spend           DECIMAL(15,2)    NOT NULL,
    proportion_of_total DOUBLE PRECISION NOT NULL,
    score               INT              NOT NULL,
    UNIQUE(customer_id, category, year_month)
);

CREATE INDEX IF NOT EXISTS idx_mcs_customer  ON monthly_category_score(customer_id);
CREATE INDEX IF NOT EXISTS idx_mcs_yearmonth ON monthly_category_score(year_month);

-- ── Aggregated scores over arbitrary time windows ────────────────────────────
CREATE TABLE IF NOT EXISTS aggregated_category_score (
    id                  BIGSERIAL        PRIMARY KEY,
    customer_id         VARCHAR(64)      NOT NULL,
    category            VARCHAR(128)     NOT NULL,
    period_label        VARCHAR(32)      NOT NULL,
    raw_spend           DECIMAL(15,2)    NOT NULL,
    proportion_of_total DOUBLE PRECISION NOT NULL,
    score               INT              NOT NULL,
    UNIQUE(customer_id, category, period_label)
);

CREATE INDEX IF NOT EXISTS idx_acs_customer ON aggregated_category_score(customer_id);
CREATE INDEX IF NOT EXISTS idx_acs_period   ON aggregated_category_score(period_label);

-- ── Known categories (reference) ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS category (
    name VARCHAR(128) PRIMARY KEY
);
