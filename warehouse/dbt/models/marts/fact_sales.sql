-- Sales fact model placeholder.
-- fact_sales.sql
select store_id, product_id, sale_date, units_sold, unit_price, revenue
from {{ ref('stg_sales') }}