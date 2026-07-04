-- perishables_risk  ⭐ the gold deliverable
--
-- One row per store x SKU x day. Joins the sell-through velocity to the
-- *current* shelf life (SCD2) and the supplier lead time, derives how many
-- days of freshness the on-hand stock has left, then scores both failure
-- modes and raises a single actionable flag.

with velocity as (
    select * from {{ ref('int_inventory_sell_through') }}
),

enriched as (
    select
        v.store_id,
        v.product_id,
        v.snapshot_date,
        v.on_hand_qty,
        v.oldest_batch_age_days,
        v.trailing_avg_daily_sell_through as sell_through,
        sl.shelf_life_days,
        sup.lead_time_days,
        -- Freshness runway of the oldest stock still on the shelf.
        greatest(sl.shelf_life_days - v.oldest_batch_age_days, 0) as days_remaining_shelf_life
    from velocity v
    join {{ ref('dim_product') }}  p  on v.product_id = p.product_id
    join {{ ref('dim_supplier') }} sup on p.supplier_id = sup.supplier_id
    join {{ ref('dim_shelf_life') }} sl on v.product_id = sl.product_id and sl.is_current
),

scored as (
    select
        *,
        {{ spoilage_risk_score('on_hand_qty', 'sell_through', 'days_remaining_shelf_life') }} as spoilage_risk_raw,
        {{ stockout_risk_score('on_hand_qty', 'sell_through', 'lead_time_days') }}            as stockout_risk_raw
    from enriched
),

rounded as (
    -- Round once, then flag on the rounded values so the flag can never
    -- disagree with the scores shown in the output (boundary-safe).
    select
        *,
        round(spoilage_risk_raw, 3) as spoilage_risk_score,
        round(stockout_risk_raw, 3) as stockout_risk_score
    from scored
)

select
    store_id,
    product_id,
    snapshot_date,
    on_hand_qty,
    days_remaining_shelf_life,
    round(sell_through, 2)                              as trailing_7d_sell_through,
    round(sell_through * days_remaining_shelf_life, 1)  as projected_demand_before_expiry,
    lead_time_days                                      as replenishment_lead_days,
    spoilage_risk_score,
    stockout_risk_score,
    -- The flag marks an *acute, actionable* state; the scores above are the
    -- continuous gradients behind it. STOCKOUT = the shelf is empty while
    -- demand is live (losing sales now); SPOILAGE = most of the on-hand stock
    -- is projected to expire unsold. Everything else is OK.
    case
        when on_hand_qty = 0 and sell_through > 0 then 'STOCKOUT'
        when spoilage_risk_score >= {{ var('spoilage_flag_threshold') }} then 'SPOILAGE'
        else 'OK'
    end                                                 as risk_flag,
    current_timestamp                                   as computed_at
from rounded