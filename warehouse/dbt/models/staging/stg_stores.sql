-- Staging stores model placeholder.
-- stg_stores.sql
-- Store dimension: one row per store.
select
    store_id,
    store_name,
    store_format,
    city,
    state,
    region,
    cast(size_factor as double) as size_factor,
    cast(open_date as date)     as open_date
from {{ source('perishables_raw', 'dim_store') }}