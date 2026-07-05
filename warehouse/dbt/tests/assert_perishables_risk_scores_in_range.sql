select *
from {{ ref('perishables_risk') }}
where spoilage_risk_score < 0
   or spoilage_risk_score > 1
   or stockout_risk_score < 0
   or stockout_risk_score > 1
