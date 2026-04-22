with base as (
    select * from {{ ref('stg_prescribers') }}
    where opioid_prescriber_rate is not null
),

specialty_avg as (
    select
        specialty,
        round(avg(opioid_prescriber_rate), 2)   as specialty_avg_rate
    from base
    group by specialty
),

prescriber_rates as (
    select
        b.prescriber_npi,
        b.last_name,
        b.first_name,
        b.specialty,
        b.state,
        round(b.opioid_prescriber_rate, 2)      as opioid_rate,
        s.specialty_avg_rate,
        round(b.opioid_prescriber_rate - s.specialty_avg_rate, 2) as deviation_from_avg
    from base b
    inner join specialty_avg s on b.specialty = s.specialty
)

select
    prescriber_npi,
    last_name,
    first_name,
    specialty,
    state,
    opioid_rate,
    specialty_avg_rate,
    deviation_from_avg,
    case
        when deviation_from_avg > 40 then 'HIGH OUTLIER'
        else 'NORMAL'
    end                                         as outlier_flag
from prescriber_rates
where deviation_from_avg > 40
order by deviation_from_avg desc
