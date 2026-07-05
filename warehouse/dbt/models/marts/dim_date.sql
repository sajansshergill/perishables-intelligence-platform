-- Date dimension model placeholder.
-- dim_date.sql
select date_day, year, month, day, day_of_week, is_weekend
from {{ ref('stg_date') }}