{{
  config({
		
		"materialized": 'ephemeral'
  })
}}

select distinct dim_video_id, video_title as title , video_length as duration, as_on_date
from {{source('cdm','dim_video')}}
where dim_video_id in 
	(select distinct dim_video_id from {{source('fds_fbk','vw_aggr_fb_daily_consumption_engagement_by_video_todate')}})