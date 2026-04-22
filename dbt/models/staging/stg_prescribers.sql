with source as (
    select * from {{ source('raw', 'part_d_prescribers') }}
),

cleaned as (
    select
        -- identifiers
        prscrbr_npi                                                         as prescriber_npi,
        prscrbr_last_org_name                                               as last_name,
        prscrbr_first_name                                                  as first_name,
        prscrbr_crdntls                                                     as credentials,
        prscrbr_ent_cd                                                      as entity_code,

        -- geography
        prscrbr_city                                                        as city,
        prscrbr_state_abrvtn                                                as state,
        prscrbr_state_fips                                                  as state_fips,
        prscrbr_zip5                                                        as zip_code,
        prscrbr_cntry                                                       as country,
        prscrbr_ruca_desc                                                   as rural_urban_desc,

        -- specialty
        prscrbr_type                                                        as specialty,
        prscrbr_type_src                                                    as specialty_source,

        -- total drug utilization
        -- CMS suppresses small values with '*' or '#'; treat as null
        try_to_number(nullif(nullif(tot_clms, '*'), '#'))                   as total_claims,
        try_to_number(nullif(nullif(tot_30day_fills, '*'), '#'), 18, 2)     as total_30day_fills,
        try_to_number(nullif(nullif(tot_drug_cst, '*'), '#'), 18, 2)        as total_drug_cost,
        try_to_number(nullif(nullif(tot_day_suply, '*'), '#'))               as total_day_supply,
        try_to_number(nullif(nullif(tot_benes, '*'), '#'))                   as total_beneficiaries,

        -- beneficiaries 65+
        try_to_number(nullif(nullif(ge65_tot_clms, '*'), '#'))              as ge65_claims,
        try_to_number(nullif(nullif(ge65_tot_drug_cst, '*'), '#'), 18, 2)   as ge65_drug_cost,
        try_to_number(nullif(nullif(ge65_tot_benes, '*'), '#'))             as ge65_beneficiaries,

        -- branded vs generic
        try_to_number(nullif(nullif(brnd_tot_clms, '*'), '#'))              as branded_claims,
        try_to_number(nullif(nullif(brnd_tot_drug_cst, '*'), '#'), 18, 2)   as branded_drug_cost,
        try_to_number(nullif(nullif(gnrc_tot_clms, '*'), '#'))              as generic_claims,
        try_to_number(nullif(nullif(gnrc_tot_drug_cst, '*'), '#'), 18, 2)   as generic_drug_cost,

        -- opioids
        try_to_number(nullif(nullif(opioid_tot_clms, '*'), '#'))            as opioid_claims,
        try_to_number(nullif(nullif(opioid_tot_drug_cst, '*'), '#'), 18, 2) as opioid_drug_cost,
        try_to_number(nullif(nullif(opioid_tot_suply, '*'), '#'))           as opioid_day_supply,
        try_to_number(nullif(nullif(opioid_tot_benes, '*'), '#'))           as opioid_beneficiaries,
        try_to_number(nullif(nullif(opioid_prscrbr_rate, '*'), '#'), 18, 6) as opioid_prescriber_rate,

        -- long-acting opioids
        try_to_number(nullif(nullif(opioid_la_tot_clms, '*'), '#'))         as opioid_la_claims,
        try_to_number(nullif(nullif(opioid_la_tot_drug_cst, '*'), '#'), 18, 2) as opioid_la_drug_cost,
        try_to_number(nullif(nullif(opioid_la_tot_benes, '*'), '#'))        as opioid_la_beneficiaries,
        try_to_number(nullif(nullif(opioid_la_prscrbr_rate, '*'), '#'), 18, 6) as opioid_la_prescriber_rate,

        -- antibiotics
        try_to_number(nullif(nullif(antbtc_tot_clms, '*'), '#'))            as antibiotic_claims,
        try_to_number(nullif(nullif(antbtc_tot_drug_cst, '*'), '#'), 18, 2) as antibiotic_drug_cost,
        try_to_number(nullif(nullif(antbtc_tot_benes, '*'), '#'))           as antibiotic_beneficiaries,

        -- beneficiary demographics
        try_to_number(nullif(nullif(bene_avg_age, '*'), '#'), 18, 6)        as avg_beneficiary_age,
        try_to_number(nullif(nullif(bene_age_lt_65_cnt, '*'), '#'))         as bene_age_lt65,
        try_to_number(nullif(nullif(bene_age_65_74_cnt, '*'), '#'))         as bene_age_65_74,
        try_to_number(nullif(nullif(bene_age_75_84_cnt, '*'), '#'))         as bene_age_75_84,
        try_to_number(nullif(nullif(bene_age_gt_84_cnt, '*'), '#'))         as bene_age_gt84,
        try_to_number(nullif(nullif(bene_feml_cnt, '*'), '#'))              as bene_female,
        try_to_number(nullif(nullif(bene_male_cnt, '*'), '#'))              as bene_male,
        try_to_number(nullif(nullif(bene_dual_cnt, '*'), '#'))              as bene_dual_eligible,
        try_to_number(nullif(nullif(bene_ndual_cnt, '*'), '#'))             as bene_non_dual

    from source
    where
        prscrbr_type is not null
        and prscrbr_state_abrvtn is not null
        and prscrbr_cntry = 'US'
)

select * from cleaned
