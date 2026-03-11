-- =============================================================================
-- V2: Add covering indexes to make stats queries index-only scans.
-- =============================================================================

-- Allows COUNT(DISTINCT customer_id) and COUNT(DISTINCT year_month) on
-- monthly_category_score to be satisfied by a single index-only scan.
CREATE INDEX IF NOT EXISTS idx_mcs_customer_month
    ON monthly_category_score(customer_id, year_month);

-- Allows COUNT(DISTINCT customer_id) on transaction to use an index-only scan.
CREATE INDEX IF NOT EXISTS idx_txn_customer_id
    ON transaction(customer_id);
