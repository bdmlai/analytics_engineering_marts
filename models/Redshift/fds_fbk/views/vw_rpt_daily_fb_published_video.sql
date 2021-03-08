{{
  config({
	'schema': 'fds_fbk',"materialized": 'view','tags': "Content","persist_docs": {'relation' : true, 'columns' : true}
	})
}}

select dim_smprovider_account_id,dim_social_account_id,dim_platform_id,dim_video_id,dim_date_id,
       date_posted,video_title,post_message,iscrosspost,post_ids_that_reused_the_video,video_length,as_on_date 
from

(select *,row_number() over(partition by dim_video_id order by dim_date_id desc) as row

from {{source('fds_fbk','fact_fb_published_video')}}) a

where row = 1

