{{
  config({
		"materialized": 'ephemeral'
  })
}}
select modified_month, region, demographic_type, demographic_group_name, sum(viewing_hours) as regional_viewing_hours
from {{ref('intm_kntr_schedule_vh_data')}}
group by 1,2,3,4