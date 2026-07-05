-- Shelf-life dimension model placeholder.
-- dim_shelf_life.sql
-- Full SCD2 history preserved; is_current marks the record in force today.
select product_id, shelf_life_days, effective_from, effective_to, is_current
from {{ ref('stg_shelf_life') }}