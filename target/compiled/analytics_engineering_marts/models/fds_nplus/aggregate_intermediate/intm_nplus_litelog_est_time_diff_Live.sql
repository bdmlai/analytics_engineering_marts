

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
where title like '%205%' and issegmentmarker='TRUE' and comment is not null
        and segmenttype not in ('Signature','Match Milestone','Set Shot','Sponsor Element','Promo Graphic') 
        and airdate in ( select airdate from __dbt__CTE__intm_nplus_content_205_Live) 
 and showdbid is not null and showdbid <> 0 and inpoint is not null 
and inpoint <> ' ' and segmenttype is not null 
and episodenumber in (select episodenumber from __dbt__CTE__intm_nplus_content_205_Live)
group by 1) a
join "entdwdb"."udl_nplus"."raw_post_event_log" b
on a.showdbid = b.showdbid and a.min_inpoint = b.inpoint) c
join "entdwdb"."udl_emm"."emm_weekly_log_reference" d 
on c.airdate = d.air_date and
lower(trim(c.logname)) = lower(trim(d.logname))
where airdate in (select airdate from __dbt__CTE__intm_nplus_content_205_Live)
 and d.start_time_eastern is not null and d.start_time_eastern not in ('0', ' ')