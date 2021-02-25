{{
  config({
		"materialized": 'ephemeral'
  })
}}
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
from {{ref('intm_le_routing_county_data')}} a 
left join {{ref('intm_le_routing_county_data')}} b on a.state = b.state and a.county_name = b.county_name and a.rank1 = b.rank1 - 1
left join {{ref('intm_le_routing_county_data')}} c on a.state = c.state and a.county_name = c.county_name and a.rank1 = c.rank1 - 14)