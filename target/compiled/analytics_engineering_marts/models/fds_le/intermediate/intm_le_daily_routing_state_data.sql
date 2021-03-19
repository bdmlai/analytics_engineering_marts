
with __dbt__CTE__intm_le_weekly_us_unemployment_data as (

select *, row_number() over (partition by state order by filed_week_ended desc) as rank
from
(select * from 
(select *, extract(week from filed_week_ended::date) as week_num, extract(year from filed_week_ended::date) as year_num,
row_number() over (partition by state, filed_week_ended order by as_on_date desc) as rank_1  
from "prod_entdwdb"."udl_cp"."le_weekly_us_unemployment_claims_details")
where rank_1 = 1)
),  __dbt__CTE__intm_le_weekly_us_unemployment_claims as (

select a.*, b.initial_claims as initial_claims_14day_prior, b.continued_claims as continued_claims_14day_prior
from __dbt__CTE__intm_le_weekly_us_unemployment_data a 
left join __dbt__CTE__intm_le_weekly_us_unemployment_data b on a.state = b.state and a.rank = b.rank - 2
)select a.*,
b.initial_claims, b.continued_claims, b.covered_employment, 
b.insured_unemployment_rate, b.initial_claims_14day_prior, b.continued_claims_14day_prior
from
(select state, date, status_of_reopening, stay_at_home_order, mandatory_quarantine_for_travelers, 
non_essential_business_closures, large_gatherings_ban, restaurant_limits, bar_closures, face_covering_requirement, face_covering_requirement_bucket_group, large_gatherings_ban_bucket_group, sum(population) as population,
sum(total_deaths) as state_total_deaths ,sum(total_cases) as state_total_cases, sum(new_deaths) as state_new_deaths,
sum(new_cases) as state_new_cases, count(distinct county_name)as no_of_county_data_available,
avg(retail_and_recreation_percent_change_from_baseline_14day_moving_avg) as avg_retail_and_recreation_percent_change_from_baseline_14day_moving_avg,
avg(grocery_and_pharmacy_percent_change_from_baseline_14day_moving_avg) as avg_grocery_and_pharmacy_percent_change_from_baseline_14day_moving_avg,
avg(parks_percent_change_from_baseline_14day_moving_avg) as avg_parks_percent_change_from_baseline_14day_moving_avg,
avg(transit_stations_percent_change_from_baseline_14day_moving_avg) as avg_transit_stations_percent_change_from_baseline_14day_moving_avg,
avg(workplaces_percent_change_from_baseline_14day_moving_avg) as avg_workplaces_percent_change_from_baseline_14day_moving_avg,
avg(residential_percent_change_from_baseline_14day_moving_avg) as avg_residential_percent_change_from_baseline_14day_moving_avg,
sum(new_cases_3day_moving_avg) as new_cases_3day_moving_avg, sum(new_cases_14day_moving_avg) as new_cases_14day_moving_avg,
sum(new_deaths_3day_moving_avg) as new_deaths_3day_moving_avg, sum(new_deaths_14day_moving_avg) as new_deaths_14day_moving_avg,
sum(new_cases_5day_moving_avg) as new_cases_5day_moving_avg, sum(new_cases_7day_moving_avg) as new_cases_7day_moving_avg,
sum(new_deaths_5day_moving_avg) as new_deaths_5day_moving_avg, sum(new_deaths_7day_moving_avg) as new_deaths_7day_moving_avg, 
sum(total_cases_14day_moving_avg) as total_cases_14day_moving_avg
from "entdwdb"."fds_le"."rpt_le_daily_routing_county_data" 
where date = (select max(date) from "entdwdb"."fds_le"."rpt_le_daily_routing_county_data")
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12) a
left join 
(select state, initial_claims, continued_claims, covered_employment, 
insured_unemployment_rate, initial_claims_14day_prior, continued_claims_14day_prior 
from __dbt__CTE__intm_le_weekly_us_unemployment_claims where rank = 1) b on trim(lower(a.state)) = trim(lower(b.state))