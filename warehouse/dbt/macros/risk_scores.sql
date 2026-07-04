{#
    Risk-scoring logic, factored into macros so the definitions live in one
    auditable place and the gold model reads like a specification.

    Both scores are bounded [0, 1] and interpretable:
      * spoilage_risk_score  -> fraction of current on-hand unlikely to sell
                                before it expires.
      * stockout_risk_score  -> how far current cover falls short of the
                                replenishment lead time.
#}

{% macro spoilage_risk_score(on_hand, sell_through, days_remaining) %}
    -- Expected units sold before expiry = velocity x days of shelf life left.
    -- Anything on-hand beyond that is at risk of being written off.
    case
        when {{ on_hand }} <= 0 then 0.0
        else least(1.0, greatest(0.0,
            ({{ on_hand }} - coalesce({{ sell_through }}, 0) * greatest({{ days_remaining }}, 0))
            / nullif({{ on_hand }}, 0)
        ))
    end
{% endmacro %}


{% macro stockout_risk_score(on_hand, sell_through, lead_days) %}
    -- Days of cover = how long current stock lasts at current velocity.
    -- If that's shorter than the lead time, the shelf empties before a
    -- reorder can land. No demand -> no stockout risk.
    case
        when coalesce({{ sell_through }}, 0) <= 0 then 0.0
        when {{ lead_days }} <= 0 then 0.0
        else least(1.0, greatest(0.0,
            ({{ lead_days }} - ({{ on_hand }} / nullif({{ sell_through }}, 0)))
            / {{ lead_days }}
        ))
    end
{% endmacro %}