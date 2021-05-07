{{
  config({
		"materialized": 'ephemeral'
  })
}}

select * from {{ref('intm_nplus_content_Live')}} a left join
(select distinct external_id, content_duration, live_start_schedule, live_end_schedule 
from {{source('fds_nplus','fact_daily_content_viewership')}} 
where external_id in (select wwe_id from {{ref('intm_nplus_content_Live')}})
and trunc(live_start_schedule) in (select airdate from {{ref('intm_nplus_content_Live')}})
)b
on a.wwe_id = b.external_id