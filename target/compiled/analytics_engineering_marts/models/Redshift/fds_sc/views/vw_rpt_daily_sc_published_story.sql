

select dim_smprovider_account_id,dim_platform_id,dim_story_id,dim_date_id,story_start,
       updated,story_view_time_secs,as_on_date 
from
(select *,row_number() over(partition by dim_story_id order by dim_date_id desc) as row

from "entdwdb"."fds_sc"."fact_sc_published_story") a
where row = 1