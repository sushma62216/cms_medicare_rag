with base as (
    select * from {{ ref('stg_prescribers') }}
    where total_drug_cost is not null
),

aggregated as (
    select
        specialty,
        count(distinct prescriber_npi)              as total_prescribers,
        sum(total_claims)                           as total_claims,
        round(sum(total_drug_cost), 2)              as total_spend,
        round(avg(total_drug_cost), 2)              as avg_spend_per_prescriber
    from base
    group by specialty
),

with_cumulative as (
    select
        specialty,
        total_prescribers,
        total_claims,
        total_spend,
        avg_spend_per_prescriber,
        round(
            sum(total_spend) over (order by total_spend desc rows between unbounded preceding and current row)
            / sum(total_spend) over () * 100,
            2
        )                                           as cumulative_pct
    from aggregated
)

select * from with_cumulative
order by total_spend desc
