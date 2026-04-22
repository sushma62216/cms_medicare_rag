with base as (
    select * from {{ ref('stg_prescribers') }}
    where total_drug_cost is not null
)

select
    state,
    count(distinct prescriber_npi)                              as total_prescribers,
    round(sum(total_drug_cost), 2)                              as total_drug_spend,
    sum(total_beneficiaries)                                    as total_beneficiaries,
    round(
        sum(total_drug_cost) / nullif(sum(total_beneficiaries), 0),
        2
    )                                                           as cost_per_beneficiary
from base
group by state
order by total_drug_spend desc
