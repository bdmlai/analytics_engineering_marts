
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
)select *,
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