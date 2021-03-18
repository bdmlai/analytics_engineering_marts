{{
  config({
		"materialized": 'ephemeral'
  })
}}

select * from {{ref('intm_nplus_content_nxt_tko')}} a left join
(select distinct external_id, content_duration, live_start_schedule, live_end_schedule 
from {{source('fds_nplus','fact_daily_content_viewership')}} 
where external_id in (select network_id from {{ref('intm_nplus_content_nxt_tko')}})
and trunc(live_start_schedule) in (select premiere_date from {{ref('intm_nplus_content_nxt_tko')}})
)b
on a.network_id = b.external_id