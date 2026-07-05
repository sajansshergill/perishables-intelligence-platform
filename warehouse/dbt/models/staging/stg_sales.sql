-- Staging sales model placeholder.
-- stg_sales.sql
-- Daily sales fact. A row exists only for days on which a sale occurred.
select
    store_id,
    product_id,
    cast(sale_date as date)      as sale_date,
    cast(units_sold as integer)  as units_sold,
    cast(unit_price as double)   as unit_price,
    cast(revenue as double)      as revenue
from {{ source('perishables_raw', 'fact_sales') }}