
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
),  __dbt__CTE__intm_le_daily_routing_state_data as (

select a.*,
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
),  __dbt__CTE__intm_le_state_regulation_change as (

select * from "entdwdb"."fds_le"."rpt_le_daily_kff_state_regulation" 
where state_regulation_update_date >= (select max(state_regulation_update_date) from "entdwdb"."fds_le"."rpt_le_daily_kff_state_regulation" ) - 7
),  __dbt__CTE__intm_le_regulation_change_gathering as (

select * from
(select *, rank() over (partition by state order by state_regulation_update_date_max desc) as regulation_change_rank 
from
(select state, large_gatherings_ban_bucket_group, max(state_regulation_update_date) as state_regulation_update_date_max 
from __dbt__CTE__intm_le_state_regulation_change
where state in (select state from 
(select * from __dbt__CTE__intm_le_state_regulation_change 
where state_regulation_update_date = (select max(state_regulation_update_date) from __dbt__CTE__intm_le_state_regulation_change)) a 
where exists (select large_gatherings_ban_bucket_group from __dbt__CTE__intm_le_state_regulation_change b 
where a.state = b.state and 
trim(replace(lower(a.large_gatherings_ban_bucket_group),' ','')) <> trim(replace(lower(b.large_gatherings_ban_bucket_group),' ','')))) 
group by 1,2
having state_regulation_update_date_max < (select max(state_regulation_update_date) from __dbt__CTE__intm_le_state_regulation_change))) 
where regulation_change_rank = 1
),  __dbt__CTE__intm_le_routing_county_data as (

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
),  __dbt__CTE__intm_le_regulation_change_facecovering as (

select * from
(select *, rank() over (partition by state order by state_regulation_update_date_max desc) as regulation_change_rank 
from
(select state, face_covering_requirement_bucket_group, max(state_regulation_update_date) as state_regulation_update_date_max 
from __dbt__CTE__intm_le_state_regulation_change 
where state in (select state from 
(select * from __dbt__CTE__intm_le_state_regulation_change 
where state_regulation_update_date = (select max(state_regulation_update_date) from __dbt__CTE__intm_le_state_regulation_change)) a 
where exists (select face_covering_requirement_bucket_group from __dbt__CTE__intm_le_state_regulation_change b 
where a.state = b.state and 
trim(replace(lower(a.face_covering_requirement_bucket_group),' ','')) <> trim(replace(lower(b.face_covering_requirement_bucket_group),' ','')))) 
group by 1,2
having state_regulation_update_date_max < (select max(state_regulation_update_date) from __dbt__CTE__intm_le_state_regulation_change))) 
where regulation_change_rank = 1
)select a.*, c.state_covid_growth_rank,
coalesce(b.large_gatherings_ban_bucket_group, a.large_gatherings_ban_bucket_group) as large_gatherings_ban_previous,
(b.state_regulation_update_date_max + 1) as large_gatherings_change_date,
coalesce(d.face_covering_requirement_bucket_group, a.face_covering_requirement_bucket_group) as face_covering_requirement_previous,
(d.state_regulation_update_date_max + 1) as face_covering_requirement_change_date,
('DBT_' || TO_CHAR(((convert_timezone ('UTC', getdate()))::timestamp),'YYYY_MM_DD_HH24_MI_SS') || '_5A')::varchar(255) as etl_batch_id, 
'SVC_PROD_BI_DBT_USER' as etl_insert_user_id, ((convert_timezone ('UTC', getdate()))::timestamp) as etl_insert_rec_dttm, 
null::varchar(255) as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from __dbt__CTE__intm_le_daily_routing_state_data a 
left join __dbt__CTE__intm_le_regulation_change_gathering b on trim(lower(a.state)) = trim(lower(b.state))
left join __dbt__CTE__intm_le_routing_state_covid_growth c on trim(lower(a.state)) = trim(lower(c.state))
left join __dbt__CTE__intm_le_regulation_change_facecovering d on trim(lower(a.state)) = trim(lower(d.state))