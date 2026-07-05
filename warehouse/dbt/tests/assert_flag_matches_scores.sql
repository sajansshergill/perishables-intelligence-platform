select *
from {{ ref('perishables_risk') }}
where (
    risk_flag = 'STOCKOUT'
    and not (on_hand_qty = 0 and trailing_7d_sell_through > 0)
)
or (
    risk_flag = 'SPOILAGE'
    and spoilage_risk_score < {{ var('spoilage_flag_threshold') }}
)
or (
    risk_flag = 'OK'
    and (
        (on_hand_qty = 0 and trailing_7d_sell_through > 0)
        or spoilage_risk_score >= {{ var('spoilage_flag_threshold') }}
    )
)
