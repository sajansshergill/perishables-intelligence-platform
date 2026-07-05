-- Supplier dimension model placeholder.
-- dim_supplier.sql
select supplier_id, supplier_name, lead_time_days, reliability
from {{ ref('stg_suppliers') }}