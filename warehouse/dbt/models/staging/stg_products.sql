-- Staging products model placeholder.
-- stg_products.sql
-- Product dimension: one row per SKU.
select
    product_id,
    product_name,
    category,
    supplier_id,
    cast(unit_price as double) as unit_price,
    cast(unit_cost as double)  as unit_cost,
    cast(popularity as double) as popularity
from {{ source('perishables_raw', 'dim_product') }}