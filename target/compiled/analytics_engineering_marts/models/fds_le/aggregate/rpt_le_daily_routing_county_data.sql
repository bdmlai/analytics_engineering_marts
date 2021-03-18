
with __dbt__CTE__intm_le_routing_county_data as (

select *, row_number() over (partition by state, county_name order by date desc) as rank1 
from 
(select * from
(select *, row_number() over(partition by state,county_fips,date order by total_cases desc) as rank_covid_dedupe_2
from
(select a.date, a.state, a.county as county_name, a.fips as county_fips, 
a.cases as total_cases, a.deaths as total_deaths, a.iso3166_1, a.iso3166_2 as state_code,
b.total_population as population, b.total_male_population, b.total_female_population,
c.grocery_and_pharmacy_change_perc as grocery_and_pharmacy_percent_change_from_baseline, 
c.parks_change_perc as parks_percent_change_from_baseline, 
c.residential_change_perc as residential_percent_change_from_baseline,
c.retail_and_recreation_change_perc as retail_and_recreation_percent_change_from_baseline, 
c.transit_stations_change_perc as transit_stations_percent_change_from_baseline, 
c.workplaces_change_perc as workplaces_percent_change_from_baseline,
d.status_of_reopening, d.stay_at_home_order, d.mandatory_quarantine_for_travelers, d.non_essential_business_closures,
d.large_gatherings_ban, d.restaurant_limits, d.bar_closures, d.face_covering_requirement, d.large_gatherings_ban_bucket_group,
d.face_covering_requirement_bucket_group
from intm_le_routing_nyt_data a
left join "starschema_aws_us_east_va_covid19_by_starschema_dm"."public"."demographics" b on trim(a.fips) = trim(b.fips)
left join "starschema_aws_us_east_va_covid19_by_starschema_dm"."public"."goog_global_mobility_report" c 
on trim(lower(a.iso3166_1)) = trim(lower(c.iso_3166_1)) and trim(lower(a.iso3166_2)) = trim(lower(c.iso_3166_2))
and trim(replace(replace(replace(replace(replace(replace(lower(a.county),'county',''),'.',''),'city', ''), 'borough', '')
,'census area', ''), 'city and borough', ''))
=trim(replace(replace(replace(replace(replace(replace(lower(c.sub_region_2),'county',''),'.',''),'city', ''), 'borough', '')
,'census area', ''),'city and borough', ''))
and a.date = c.date + (select (dateadd('day', -1, current_date) - max(date)) 
from "starschema_aws_us_east_va_covid19_by_starschema_dm"."public"."goog_global_mobility_report")
left join "entdwdb"."fds_le"."rpt_le_daily_kff_state_regulation" d 
on trim(lower(a.state)) = trim(lower(d.state)) and a.date = d.state_regulation_update_date))
where rank_covid_dedupe_2 = 1)
),  __dbt__CTE__intm_le_routing_county_cases_deaths as (

select *,
avg(retail_and_recreation_percent_change_from_baseline) over(partition by state, county_name order by date asc rows 13 preceding) as retail_and_recreation_percent_change_from_baseline_14day_moving_avg,
avg(grocery_and_pharmacy_percent_change_from_baseline) over(partition by state, county_name order by date asc rows 13 preceding) as grocery_and_pharmacy_percent_change_from_baseline_14day_moving_avg,
avg(parks_percent_change_from_baseline) over(partition by state, county_name order by date asc rows 13 preceding) as parks_percent_change_from_baseline_14day_moving_avg,
avg(transit_stations_percent_change_from_baseline) over(partition by state, county_name order by date asc rows 13 preceding) as transit_stations_percent_change_from_baseline_14day_moving_avg,
avg(workplaces_percent_change_from_baseline) over(partition by state, county_name order by date asc rows 13 preceding) as workplaces_percent_change_from_baseline_14day_moving_avg,
avg(residential_percent_change_from_baseline) over(partition by state, county_name order by date asc rows 13 preceding) as residential_percent_change_from_baseline_14day_moving_avg,
avg(total_deaths) over(partition by state,county_name order by date asc rows 13 preceding) as total_deaths_14day_moving_avg,
avg(total_cases) over(partition by state,county_name order by date asc rows 13 preceding) as total_cases_14day_moving_avg,
avg(new_deaths) over(partition by state,county_name order by date asc rows 13 preceding) as new_deaths_14day_moving_avg,
avg(new_cases) over(partition by state,county_name order by date asc rows 13 preceding) as new_cases_14day_moving_avg,
avg(total_deaths) over(partition by state,county_name order by date asc rows 2 preceding) as total_deaths_3day_moving_avg,
avg(total_cases) over(partition by state,county_name order by date asc rows 2 preceding) as total_cases_3day_moving_avg,
avg(new_deaths) over(partition by state,county_name order by date asc rows 6 preceding) as new_deaths_7day_moving_avg,
avg(new_cases) over(partition by state,county_name order by date asc rows 6 preceding) as new_cases_7day_moving_avg,
avg(new_deaths) over(partition by state,county_name order by date asc rows 4 preceding) as new_deaths_5day_moving_avg,
avg(new_cases) over(partition by state,county_name order by date asc rows 4 preceding) as new_cases_5day_moving_avg,
avg(new_deaths) over(partition by state,county_name order by date asc rows 2 preceding) as new_deaths_3day_moving_avg,
avg(new_cases) over(partition by state,county_name order by date asc rows 2 preceding) as new_cases_3day_moving_avg
from 
(select a.*, b.total_deaths as prev_day_deaths, b.total_cases as prev_day_cases,
greatest(a.total_deaths - b.total_deaths, 0) as new_deaths, greatest(a.total_cases - b.total_cases,0) as new_cases,
c.total_deaths as total_deaths_14day_prior, c.total_cases as total_cases_14day_prior,
greatest(a.total_deaths - c.total_deaths, 0) as total_deaths_14day_prior_diff, 
greatest(a.total_cases - c.total_cases, 0) as total_cases_14day_prior_diff
from __dbt__CTE__intm_le_routing_county_data a 
left join __dbt__CTE__intm_le_routing_county_data b on a.state = b.state and a.county_name = b.county_name and a.rank1 = b.rank1 - 1
left join __dbt__CTE__intm_le_routing_county_data c on a.state = c.state and a.county_name = c.county_name and a.rank1 = c.rank1 - 14)
),  __dbt__CTE__intm_le_routing_county_covid_growth as (

select *, extract(week from date) as week_num, extract(year from date) as year_num,
case 
when (100 * (new_cases_3day_moving_avg - new_cases_14day_moving_avg))  <= (-50 * new_cases_14day_moving_avg) then 'Dark Green'  
when (100 * (new_cases_3day_moving_avg - new_cases_14day_moving_avg))  <= (-10 * new_cases_14day_moving_avg) then 'Light Green'  
when (100 * (new_cases_3day_moving_avg - new_cases_14day_moving_avg))  <  ( 10 * new_cases_14day_moving_avg) then 'Grey'
when (100 * (new_cases_3day_moving_avg - new_cases_14day_moving_avg))  <  ( 50 * new_cases_14day_moving_avg) then 'Light Red'
else 'Dark Red'  
end as covid_growth_indicator
from __dbt__CTE__intm_le_routing_county_cases_deaths
),  __dbt__CTE__intm_le_routing_state_covid_growth as (

select *, dense_rank() over (order by (100 * (new_cases_3day_moving_avg - new_cases_14day_moving_avg) / nullif(new_cases_14day_moving_avg, 0)) desc nulls last) as state_covid_growth_rank 
from 
(select state, sum(total_cases) as total_cases, sum(new_cases_3day_moving_avg) as new_cases_3day_moving_avg, 
sum(new_cases_14day_moving_avg) as new_cases_14day_moving_avg
from __dbt__CTE__intm_le_routing_county_covid_growth where rank1 = 1 group by state)
),  __dbt__CTE__intm_le_weekly_us_unemployment_data as (

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
)select a.date, a.iso3166_1 as country_code, a.state_code, a.state::varchar(255) as state, a.county_name::varchar(255) as county_name, 
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
from __dbt__CTE__intm_le_routing_county_covid_growth a
left join __dbt__CTE__intm_le_routing_state_covid_growth e    on a.state = e.state
left join __dbt__CTE__intm_le_weekly_us_unemployment_claims f 
on trim(lower(a.state)) = trim(lower(f.state)) and a.week_num = f.week_num and a.year_num = f.year_num
left join 
(select state, initial_claims, continued_claims 
from __dbt__CTE__intm_le_weekly_us_unemployment_claims where rank = 1) d on trim(lower(a.state)) = trim(lower(d.state))