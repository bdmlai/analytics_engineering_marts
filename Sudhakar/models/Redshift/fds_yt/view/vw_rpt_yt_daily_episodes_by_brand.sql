{{
  config({
	'schema': 'fds_yt',"materialized": 'view','tags': "Content","persist_docs": {'relation' : true, 'columns' : true},
          'post-hook': 'grant select on fds_yt.vw_rpt_yt_daily_episodes_by_brand to public'
       
	})
}}


select channel_id
	,channel_name
	,video_id
	,title
	,country_code
	,time_uploaded
	,report_date_dt
	,views
	,comments
	,likes
	,dislikes
	,shares
	,brand
	,playlist_title
	,day_uploaded
	,episode_air_date
	,"date flag"
	,week_uploaded
	,"pl flag"
	,"table type"
from {{ref('rpt_yt_daily_episodes_by_brand')}}
