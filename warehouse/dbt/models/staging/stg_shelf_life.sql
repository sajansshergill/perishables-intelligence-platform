-- Staging shelf-life model placeholder.
-- stg_shelf_life.sql
-- Shelf-life reference as a Type-2 slowly-changing dimension: full history,
-- with is_current marking the record in force today.
select
    product_id,
    cast(shelf_life_days as integer) as shelf_life_days,
    cast(effective_from as date)     as effective_from,
    cast(effective_to as date)       as effective_to,
    cast(is_current as boolean)      as is_current
from {{ source('perishables_raw', 'dim_shelf_life') }}