

select dim_smprovider_account_id,dim_social_account_id,dim_platform_id,dim_video_id,dim_media_id,
       dim_content_type_id,dim_date_id,post_date,caption,content_published_post_time,as_on_date 
from
(select *,row_number() over(partition by dim_video_id,dim_media_id order by dim_date_id desc) as row

from "entdwdb"."fds_igm"."fact_ig_published_post") a
where row = 1