with base as (
    select * from {{ ref('stg_prescribers') }}
    where total_drug_cost is not null
)

select
    specialty,
    round(sum(total_drug_cost), 2)                              as total_spend,
    round(sum(branded_drug_cost), 2)                            as branded_spend,
    round(sum(generic_drug_cost), 2)                            as generic_spend,
    round(
        sum(branded_drug_cost) / nullif(sum(total_drug_cost), 0) * 100,
        2
    )                                                           as branded_pct,
    round(
        sum(generic_drug_cost) / nullif(sum(total_drug_cost), 0) * 100,
        2
    )                                                           as generic_pct
from base
where branded_drug_cost is not null
  and generic_drug_cost is not null
group by specialty
order by total_spend desc
