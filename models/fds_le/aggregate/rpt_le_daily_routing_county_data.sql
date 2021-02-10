{{
  config({
		"schema": 'fds_le',
		"materialized": 'incremental','tags': 'Phase 5A',"persist_docs": {'relation' : true, 'columns' : true}
  })
}}
select a.date, a.iso3166_1 as country_code, a.state_code, a.state::varchar(255) as state, a.county_name::varchar(255) as county_name, 
a.county_fips, a.total_cases, a.total_deaths, a.population, a.total_male_population, total_female_population,
a.grocery_and_pharmacy_percent_change_from_baseline, a.parks_percent_change_from_baseline, a.residential_percent_change_from_baseline,
a.retail_and_recreation_percent_change_from_baseline, a.transit_stations_percent_change_from_baseline, a.workplaces_percent_change_from_baseline,
a.status_of_reopening, a.stay_at_home_order, a.mandatory_quarantine_for_travelers, a.non_essential_business_closures, a.large_gatherings_ban,
a.restaurant_limits, a.bar_closures, a.face_covering_requirement, a.large_gatherings_ban_bucket_group, a.face_covering_requirement_bucket_group,
a.rank1 as date_rank, a.prev_day_deaths, a.prev_day_cases, a.new_deaths, a.new_cases, a.total_deaths_14day_prior, a.total_cases_14day_prior,
a.total_deaths_14day_prior_diff, a.total_cases_14day_prior_diff, a.retail_and_recreation_percent_change_from_baseline_14day_moving_avg,
a.grocery_and_pharmacy_percent_change_from_baseline_14day_moving_avg, a.parks_percent_change_from_baseline_14day_moving_avg,
a.transit_stations_percent_change_from_baseline_14day_moving_avg, a.workplaces_percent_change_from_baseline_14day_moving_avg,
a.residential_percent_change_from_baseline_14day_moving_avg, a.total_deaths_14day_moving_avg, a.total_cases_14day_moving_avg,
a.new_deaths_14day_moving_avg, a.new_cases_14day_moving_avg, a.total_deaths_3day_moving_avg, a.total_cases_3day_moving_avg,
a.new_deaths_7day_moving_avg, a.new_cases_7day_moving_avg, a.new_deaths_5day_moving_avg, a.new_cases_5day_moving_avg, 
a.new_deaths_3day_moving_avg, a.new_cases_3day_moving_avg, a.week_num, a.year_num, a.covid_growth_indicator, 
e.state_covid_growth_rank, f.initial_claims, f.continued_claims, f.covered_employment, 
f.insured_unemployment_rate, f.initial_claims_14day_prior, f.continued_claims_14day_prior,
d.initial_claims as initial_claims_latest_week, d.continued_claims as continued_claim_latest_week,
('DBT_' || TO_CHAR(((convert_timezone ('UTC', getdate()))::timestamp),'YYYY_MM_DD_HH24_MI_SS') || '_5A')::varchar(255) as etl_batch_id, 
'SVC_PROD_BI_DBT_USER' as etl_insert_user_id, ((convert_timezone ('UTC', getdate()))::timestamp) as etl_insert_rec_dttm, 
null::varchar(255) as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from {{ref('intm_le_routing_county_covid_growth')}} a
left join {{ref('intm_le_routing_state_covid_growth')}} e    on a.state = e.state
left join {{ref('intm_le_weekly_us_unemployment_claims')}} f 
on trim(lower(a.state)) = trim(lower(f.state)) and a.week_num = f.week_num and a.year_num = f.year_num
left join 
(select state, initial_claims, continued_claims 
from {{ref('intm_le_weekly_us_unemployment_claims')}} where rank = 1) d on trim(lower(a.state)) = trim(lower(d.state))