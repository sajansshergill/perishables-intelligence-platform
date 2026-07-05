select store_id, product_id, snapshot_date, count(*) as n
from {{ ref('perishables_risk') }}
group by 1, 2, 3
having count(*) > 1
