{{
  config({
		"materialized": 'ephemeral'
  })
}}
select * from
(select *, rank() over (partition by state order by state_regulation_update_date_max desc) as regulation_change_rank 
from
(select state, large_gatherings_ban_bucket_group, max(state_regulation_update_date) as state_regulation_update_date_max 
from {{ref('intm_le_state_regulation_change')}}
where state in (select state from 
(select * from {{ref('intm_le_state_regulation_change')}} 
where state_regulation_update_date = (select max(state_regulation_update_date) from {{ref('intm_le_state_regulation_change')}})) a 
where exists (select large_gatherings_ban_bucket_group from {{ref('intm_le_state_regulation_change')}} b 
where a.state = b.state and 
trim(replace(lower(a.large_gatherings_ban_bucket_group),' ','')) <> trim(replace(lower(b.large_gatherings_ban_bucket_group),' ','')))) 
group by 1,2
having state_regulation_update_date_max < (select max(state_regulation_update_date) from {{ref('intm_le_state_regulation_change')}}))) 
where regulation_change_rank = 1