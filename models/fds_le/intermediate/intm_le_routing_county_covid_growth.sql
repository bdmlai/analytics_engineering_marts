{{
  config({
		"materialized": 'ephemeral'
  })
}}
select *, extract(week from date) as week_num, extract(year from date) as year_num,
case 
when (100 * (new_cases_3day_moving_avg - new_cases_14day_moving_avg))  <= (-50 * new_cases_14day_moving_avg) then 'Dark Green'  
when (100 * (new_cases_3day_moving_avg - new_cases_14day_moving_avg))  <= (-10 * new_cases_14day_moving_avg) then 'Light Green'  
when (100 * (new_cases_3day_moving_avg - new_cases_14day_moving_avg))  <  ( 10 * new_cases_14day_moving_avg) then 'Grey'
when (100 * (new_cases_3day_moving_avg - new_cases_14day_moving_avg))  <  ( 50 * new_cases_14day_moving_avg) then 'Light Red'
else 'Dark Red'  
end as covid_growth_indicator
from {{ref('intm_le_routing_county_cases_deaths')}}