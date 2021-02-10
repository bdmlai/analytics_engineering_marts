{{
  config({
		"materialized": 'ephemeral'
  })
}}
select *, dense_rank() over (order by (100 * (new_cases_3day_moving_avg - new_cases_14day_moving_avg) / nullif(new_cases_14day_moving_avg, 0)) desc nulls last) as state_covid_growth_rank 
from 
(select state, sum(total_cases) as total_cases, sum(new_cases_3day_moving_avg) as new_cases_3day_moving_avg, 
sum(new_cases_14day_moving_avg) as new_cases_14day_moving_avg
from {{ref('intm_le_routing_county_covid_growth')}} where rank1 = 1 group by state)