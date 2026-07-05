-- Store dimension.
select store_id, store_name, store_format, city, state, region, size_factor, open_date
from {{ ref('stg_stores') }}
