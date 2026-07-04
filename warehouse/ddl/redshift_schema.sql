-- ============================================================================
--  Redshift raw / landing schema  (production target)
-- ----------------------------------------------------------------------------
--  In production the Glue jobs COPY the bronze Parquet into these tables and
--  dbt builds the same staging -> marts -> gold DAG on top of them. Distribution
--  and sort keys are chosen for the platform's dominant access pattern: the
--  daily risk scan joins facts to dimensions on the entity keys and filters /
--  orders by date.
--
--    * DISTKEY(product_id) co-locates each SKU's sales and inventory on the same
--      slice, so the fact<->fact and fact<->dim joins stay slice-local.
--    * SORTKEY(snapshot_date / sale_date) makes the "latest day" and trailing-
--      window scans range-restricted instead of full table reads.
--    * Small dimensions use DISTSTYLE ALL so they replicate to every node and
--      never trigger a redistribution during joins.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS raw;

-- ---------------------------------------------------------------------------
-- Dimensions  (small -> replicate everywhere)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.dim_store (
    store_id      VARCHAR(8)   NOT NULL,
    store_name    VARCHAR(128),
    store_format  VARCHAR(64),
    city          VARCHAR(64),
    state         VARCHAR(8),
    region        VARCHAR(32),
    size_factor   DOUBLE PRECISION,
    open_date     DATE
)
DISTSTYLE ALL
SORTKEY (store_id);

CREATE TABLE IF NOT EXISTS raw.dim_product (
    product_id    VARCHAR(10)  NOT NULL,
    product_name  VARCHAR(128),
    category      VARCHAR(32),
    supplier_id   VARCHAR(8),
    unit_price    DECIMAL(10,2),
    unit_cost     DECIMAL(10,2),
    popularity    DOUBLE PRECISION
)
DISTSTYLE ALL
SORTKEY (product_id);

CREATE TABLE IF NOT EXISTS raw.dim_supplier (
    supplier_id     VARCHAR(8) NOT NULL,
    supplier_name   VARCHAR(128),
    lead_time_days  INTEGER,
    reliability     DOUBLE PRECISION
)
DISTSTYLE ALL
SORTKEY (supplier_id);

-- Type-2 slowly-changing shelf-life history.
CREATE TABLE IF NOT EXISTS raw.dim_shelf_life (
    product_id       VARCHAR(10) NOT NULL,
    shelf_life_days  INTEGER,
    effective_from   DATE,
    effective_to     DATE,
    is_current       BOOLEAN
)
DISTSTYLE ALL
SORTKEY (product_id, effective_from);

CREATE TABLE IF NOT EXISTS raw.dim_date (
    date         DATE NOT NULL,
    year         INTEGER,
    month        INTEGER,
    day          INTEGER,
    day_of_week  VARCHAR(16),
    is_weekend   BOOLEAN
)
DISTSTYLE ALL
SORTKEY (date);

-- ---------------------------------------------------------------------------
-- Facts  (large -> distribute by SKU, sort by date)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.fact_sales (
    store_id    VARCHAR(8)  NOT NULL,
    product_id  VARCHAR(10) NOT NULL,
    sale_date   DATE        NOT NULL,
    units_sold  INTEGER,
    unit_price  DECIMAL(10,2),
    revenue     DECIMAL(12,2)
)
DISTSTYLE KEY
DISTKEY (product_id)
SORTKEY (sale_date);

CREATE TABLE IF NOT EXISTS raw.fact_inventory_snapshot (
    store_id               VARCHAR(8)  NOT NULL,
    product_id             VARCHAR(10) NOT NULL,
    snapshot_date          DATE        NOT NULL,
    on_hand_qty            INTEGER,
    received_qty           INTEGER,
    oldest_batch_age_days  INTEGER,
    spoiled_qty            INTEGER,
    unmet_demand           INTEGER
)
DISTSTYLE KEY
DISTKEY (product_id)
SORTKEY (snapshot_date);

-- ---------------------------------------------------------------------------
-- Load pattern (illustrative) — Glue writes partitioned Parquet to bronze,
-- then COPY pulls it in. Redshift prunes to the loaded partition on scan.
-- ---------------------------------------------------------------------------
-- COPY raw.fact_inventory_snapshot
-- FROM 's3://perishables-lake/bronze/inventory/dt=2026-07-30/'
-- IAM_ROLE 'arn:aws:iam::<acct>:role/perishables-redshift-copy'
-- FORMAT AS PARQUET;