{{
  config({
	'schema': 'fds_sc',"materialized": 'view','tags': "Content","persist_docs": {'relation' : true, 'columns' : true}
	})
}}

select dim_smprovider_account_id,dim_social_account_id,dim_platform_id,dim_story_id,dim_content_type_id,
       dim_video_id,dim_media_id,dim_date_id,frame,snap_time_posted,as_on_date 
from
(select *,row_number() over(partition by dim_video_id,dim_media_id order by dim_date_id desc) as row

from {{source('fds_sc','fact_sc_published_frame')}}) a
where row = 1