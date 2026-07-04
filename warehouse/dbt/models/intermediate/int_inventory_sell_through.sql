-- int_inventory_sell_through
--
-- Estimating demand velocity is the crux of both risk scores, and it has a
-- subtle trap: fact_sales only has rows for days a sale happened, so a naive
-- average over sales rows silently ignores zero-sale days and overstates
-- velocity. The inventory snapshot, by contrast, is dense (every store x SKU x
-- day), so we use it as the spine and LEFT JOIN sales onto it — days with no
-- sale become an explicit zero. The trailing average is then a true daily
-- sell-through over a real calendar window.

with inventory as (
    select * from {{ ref('stg_inventory_snapshot') }}
),

sales as (
    select store_id, product_id, sale_date, units_sold
    from {{ ref('stg_sales') }}
),

dense as (
    select
        inv.store_id,
        inv.product_id,
        inv.snapshot_date,
        inv.on_hand_qty,
        inv.received_qty,
        inv.oldest_batch_age_days,
        inv.spoiled_qty,
        inv.unmet_demand,
        coalesce(s.units_sold, 0) as units_sold   -- zero-sale days made explicit
    from inventory inv
    left join sales s
        on  inv.store_id     = s.store_id
        and inv.product_id   = s.product_id
        and inv.snapshot_date = s.sale_date
)

select
    *,
    avg(units_sold) over (
        partition by store_id, product_id
        order by snapshot_date
        rows between {{ var('sell_through_window_days') - 1 }} preceding and current row
    ) as trailing_avg_daily_sell_through
from dense