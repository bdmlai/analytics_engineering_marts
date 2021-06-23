{{
  config({
		"materialized": 'ephemeral'
  })
}}
select distinct c.showdbid, datediff(sec, (c.airdate || ' ' || substring(trim(c.min_inpoint), 1, 8))::timestamp, 
(d.air_date || ' ' || substring(trim(d.start_time_eastern), 1, 8))::timestamp) as est_time_diff 
from
(select distinct a.showdbid, a.min_inpoint, b.airdate, b.logname
from
(select showdbid, min(inpoint) as min_inpoint
from {{source('udl_nplus','raw_lite_log')}}
where lower(trim(title)) in ('nxt','raw','smackdown') and 
showdbid is not null and showdbid <> 0 and inpoint is not null 
and inpoint <> ' ' and segmenttype is not null 
group by 1) a
join {{source('udl_nplus','raw_lite_log')}} b
on a.showdbid = b.showdbid and a.min_inpoint = b.inpoint) c
join {{source('udl_emm','emm_weekly_log_reference')}} d on c.airdate = d.air_date and
lower(trim(c.logname)) = lower(trim(d.logname))
where d.start_time_eastern is not null and d.start_time_eastern not in ('0', ' ') 

