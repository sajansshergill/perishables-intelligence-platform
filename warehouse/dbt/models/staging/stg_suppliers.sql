-- Staging suppliers model placeholder.
-- stg_suppliers.sql
-- Supplier dimension: replenishment lead time drives stockout risk.
select
    supplier_id,
    supplier_name,
    cast(lead_time_days as integer) as lead_time_days,
    cast(reliability as double)     as reliability
from {{ source('perishables_raw', 'dim_supplier') }}