{{
  config({
		"materialized": 'ephemeral'
  })
}}
select a.*, (max_cum_unique_users::float/ prev_max_cum_unique_users::float-1) as pct_change
from {{ref('intm_nplus_max_cum_users_live')}} a
left join
(select airdate, external_id, (lag(cumulative_unique_user,1) over (order by airdate asc,time_interval asc)) prev_max_cum_unique_users
 from
(select airdate, external_id, min(time_interval) as time_interval,max(cumulative_unique_user) as cumulative_unique_user
 from {{ref('intm_nplus_max_cum_users_live')}} group by 1,2))b
on a.external_id = b.external_id and a.airdate = b.airdate
