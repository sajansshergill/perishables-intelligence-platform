-- Product dimension.
select
    p.product_id, p.product_name, p.category,
    p.supplier_id, s.supplier_name,
    p.unit_price, p.unit_cost,
    round(p.unit_price - p.unit_cost, 2) as unit_margin,
    p.popularity
from {{ ref('stg_products') }} p
left join {{ ref('stg_suppliers') }} s using (supplier_id)