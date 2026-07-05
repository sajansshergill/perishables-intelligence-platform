-- Staging date model placeholder.
-- stg_date.sql
-- Calendar dimension.
select
    cast(date as date) as date_day,
    year,
    month,
    day,
    day_of_week,
    cast(is_weekend as boolean) as is_weekend
from {{ source('perishables_raw', 'dim_date') }}