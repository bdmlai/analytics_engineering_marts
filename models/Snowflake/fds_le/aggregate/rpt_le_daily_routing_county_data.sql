{{
  config({
		"schema": 'fds_le',
		"materialized": 'table','tags': 'Phase 5A',"persist_docs": {'relation' : true, 'columns' : true},
		"post-hook": 'grant select on {{this}} to public'
  })
}}
SELECT
    a.date,
    a.iso3166_1 AS country_code,
    a.state_code,
    a.state::VARCHAR(255)       AS state,
    a.county_name::VARCHAR(255) AS county_name,
    a.county_fips,
    a.total_cases,
    a.total_deaths,
    a.population,
    a.total_male_population,
    total_female_population,
    a.grocery_and_pharmacy_percent_change_from_baseline,
    a.parks_percent_change_from_baseline,
    a.residential_percent_change_from_baseline,
    a.retail_and_recreation_percent_change_from_baseline,
    a.transit_stations_percent_change_from_baseline,
    a.workplaces_percent_change_from_baseline,
    a.status_of_reopening,
    a.stay_at_home_order,
    a.mandatory_quarantine_for_travelers,
    a.non_essential_business_closures,
    a.large_gatherings_ban,
    a.restaurant_limits,
    a.bar_closures,
    a.face_covering_requirement,
    a.large_gatherings_ban_bucket_group,
    a.face_covering_requirement_bucket_group,
    a.rank1 AS date_rank,
    a.prev_day_deaths,
    a.prev_day_cases,
    a.new_deaths,
    a.new_cases,
    a.total_deaths_14day_prior,
    a.total_cases_14day_prior,
    a.total_deaths_14day_prior_diff,
    a.total_cases_14day_prior_diff,
    a.retail_and_recreation_percent_change_from_baseline_14day_moving_avg,
    a.grocery_and_pharmacy_percent_change_from_baseline_14day_moving_avg,
    a.parks_percent_change_from_baseline_14day_moving_avg,
    a.transit_stations_percent_change_from_baseline_14day_moving_avg,
    a.workplaces_percent_change_from_baseline_14day_moving_avg,
    a.residential_percent_change_from_baseline_14day_moving_avg,
    a.total_deaths_14day_moving_avg,
    a.total_cases_14day_moving_avg,
    a.new_deaths_14day_moving_avg,
    a.new_cases_14day_moving_avg,
    a.total_deaths_3day_moving_avg,
    a.total_cases_3day_moving_avg,
    a.new_deaths_7day_moving_avg,
    a.new_cases_7day_moving_avg,
    a.new_deaths_5day_moving_avg,
    a.new_cases_5day_moving_avg,
    a.new_deaths_3day_moving_avg,
    a.new_cases_3day_moving_avg,
    a.week_num,
    a.year_num,
    a.covid_growth_indicator,
    e.state_covid_growth_rank,
    f.initial_claims,
    f.continued_claims,
    f.covered_employment,
    f.insured_unemployment_rate,
    f.initial_claims_14day_prior,
    f.continued_claims_14day_prior,
    d.initial_claims   AS initial_claims_latest_week,
    d.continued_claims AS continued_claim_latest_week,
    ('DBT_' || TO_CHAR(((convert_timezone ('UTC', GETDATE()))::TIMESTAMP),'YYYY_MM_DD_HH24_MI_SS')
    || '_5A')::VARCHAR(255)                            AS etl_batch_id,
    'SVC_PROD_BI_DBT_USER'                             AS etl_insert_user_id,
    ((convert_timezone ('UTC', GETDATE()))::TIMESTAMP) AS etl_insert_rec_dttm,
    NULL::VARCHAR(255)                                 AS etl_update_user_id,
    CAST(NULL AS TIMESTAMP)                            AS etl_update_rec_dttm
FROM
    {{ref('intm_le_routing_county_covid_growth')}} a
LEFT JOIN
    {{ref('intm_le_routing_state_covid_growth')}} e
ON
    a.state = e.state
LEFT JOIN
    {{ref('intm_le_weekly_us_unemployment_claims')}} f
ON
    trim(LOWER(a.state)) = trim(LOWER(f.state))
AND a.week_num = f.week_num
AND a.year_num = f.year_num
LEFT JOIN
    (
        SELECT
            state,
            initial_claims,
            continued_claims
        FROM
            {{ref('intm_le_weekly_us_unemployment_claims')}}
        WHERE
            rank = 1) d
ON
    trim(LOWER(a.state)) = trim(LOWER(d.state))