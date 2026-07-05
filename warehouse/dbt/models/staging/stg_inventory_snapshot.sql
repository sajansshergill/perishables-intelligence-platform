-- Staging inventory snapshot model placeholder.
-- stg_inventory_snapshot.sql
-- End-of-day inventory position for every store x SKU x day (dense).
select
    store_id,
    product_id,
    cast(snapshot_date as date)            as snapshot_date,
    cast(on_hand_qty as integer)           as on_hand_qty,
    cast(received_qty as integer)          as received_qty,
    cast(oldest_batch_age_days as integer) as oldest_batch_age_days,
    cast(spoiled_qty as integer)           as spoiled_qty,
    cast(unmet_demand as integer)          as unmet_demand
from {{ source('perishables_raw', 'fact_inventory_snapshot') }}