-- Inventory snapshot fact model placeholder.
-- fact_inventory_snapshot.sql
select
    store_id, product_id, snapshot_date,
    on_hand_qty, received_qty, oldest_batch_age_days,
    spoiled_qty, unmet_demand
from {{ ref('stg_inventory_snapshot') }}