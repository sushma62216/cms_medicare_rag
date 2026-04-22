with base as (
    select * from {{ ref('stg_prescribers') }}
    where total_drug_cost is not null
)

select
    state,
    specialty,
    count(distinct prescriber_npi)                              as total_prescribers,
    round(sum(total_drug_cost), 2)                              as total_drug_spend,
    sum(total_beneficiaries)                                    as total_beneficiaries,
    round(
        sum(total_drug_cost) / nullif(sum(total_beneficiaries), 0),
        2
    )                                                           as cost_per_beneficiary,
    round(sum(branded_drug_cost), 2)                            as branded_spend,
    round(sum(generic_drug_cost), 2)                            as generic_spend,
    round(
        sum(branded_drug_cost) / nullif(sum(total_drug_cost), 0) * 100,
        2
    )                                                           as branded_pct,
    round(avg(opioid_prescriber_rate), 2)                       as avg_opioid_rate
from base
group by state, specialty
order by total_drug_spend desc
