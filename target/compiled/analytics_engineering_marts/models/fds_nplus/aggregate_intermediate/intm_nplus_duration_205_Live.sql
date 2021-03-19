

select * from __dbt__CTE__intm_nplus_content_205_Live a left join
(select distinct external_id, content_duration, live_start_schedule, live_end_schedule 
from "entdwdb"."fds_nplus"."fact_daily_content_viewership" 
where external_id in (select wwe_id from __dbt__CTE__intm_nplus_content_205_Live)
and trunc(live_start_schedule) in (select airdate from __dbt__CTE__intm_nplus_content_205_Live)
)b
on a.wwe_id = b.external_id