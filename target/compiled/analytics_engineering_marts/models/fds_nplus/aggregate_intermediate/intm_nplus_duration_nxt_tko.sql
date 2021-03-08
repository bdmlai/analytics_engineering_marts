

select * from __dbt__CTE__intm_nplus_content_nxt_tko a left join
(select distinct external_id, content_duration, live_start_schedule, live_end_schedule 
from "entdwdb"."fds_nplus"."fact_daily_content_viewership" 
where external_id in (select network_id from __dbt__CTE__intm_nplus_content_nxt_tko)
and trunc(live_start_schedule) in (select premiere_date from __dbt__CTE__intm_nplus_content_nxt_tko)
)b
on a.network_id = b.external_id