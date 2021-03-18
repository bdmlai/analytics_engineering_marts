

select distinct c.showdbid,c.min_inpoint,d.start_time_eastern,
dateadd(hr,12,(airdate || ' ' || substring(trim(c.min_inpoint), 1, 8))::timestamp) as hr_min_inpoint,
         datediff(sec,  dateadd(hr,12,(airdate || ' ' || substring(trim(c.min_inpoint), 1, 8))::timestamp), 
         dateadd(hr,12,(d.air_date || ' ' || substring(trim(d.start_time_eastern), 1, 8))::timestamp))
 as est_time_diff_1,
  (round(est_time_diff_1/900,0)*900)::integer as est_time_diff
from 
(select distinct a.showdbid, a.min_inpoint, b.airdate, b.logname
from
(select showdbid, min(inpoint) as min_inpoint
from "entdwdb"."udl_nplus"."raw_post_event_log"
where lower(title) not like '%%kickoff%%' and lower(title) not like '%%nxt%%' and lower(title) not like '%%raw%%'
        and lower(title) not like '%%smackdown%%' and lower(title) not like '%%205 live%%' and lower(title) not like '%%king%%'
        and lower(title) not like '%%tribute%%' and lower(title) not like '%%mixed match%%' and issegmentmarker='TRUE'
        and upper(segmenttype) in ('MATCH','BACKSTAGE ON CAMERA','IN-ARENA NON-MATCH','ON CAMERA','TALK SHOW') 
        and airdate in (select premiere_date from __dbt__CTE__intm_nplus_content_duration)
 and showdbid is not null and showdbid <> 0 and inpoint is not null 
and inpoint <> ' ' and segmenttype is not null 
group by 1) a
join "entdwdb"."udl_nplus"."raw_post_event_log" b
on a.showdbid = b.showdbid and a.min_inpoint = b.inpoint) c
join "entdwdb"."udl_emm"."emm_weekly_log_reference" d 
on c.airdate = d.air_date and
lower(trim(c.logname)) = lower(trim(d.logname))
where airdate in ( select premiere_date from __dbt__CTE__intm_nplus_content_duration)
 and d.start_time_eastern is not null and d.start_time_eastern not in ('0', ' ')