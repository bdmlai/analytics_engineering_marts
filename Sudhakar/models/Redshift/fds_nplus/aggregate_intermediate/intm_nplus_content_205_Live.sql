{{
  config({
		"materialized": 'ephemeral'
  })
}}

select distinct airdate,episodenumber, 'wwetof0'+ lpad(episodenumber,3,'0') as wwe_id, '205 LIVE '+ lpad(episodenumber,3,'0') as series_episode 
from {{source('udl_nplus','raw_post_event_log')}}
where (title like '%205%') 
and (airdate >= current_date-7 and  airdate <=current_date)
order by episodenumber