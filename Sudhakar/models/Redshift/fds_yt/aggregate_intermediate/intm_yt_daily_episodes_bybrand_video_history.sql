{{
  config({
		"materialized": 'ephemeral'
        })
}}

select  channel_id
	, channel_name
	, video_id
	, title
	, country_code
	, time_uploaded
	, report_date_dt
	, views
	, comments
	, likes
	, dislikes
	, shares
       
from 
   (
     select channel_id, channel_name, video_id, title,country_code
           ,time_uploaded,as_on_date_dt, report_date_dt
           , sum(views) as views, sum(comments) as comments 
           ,sum(likes) as likes, sum(dislikes) as dislikes 
           ,sum(shares) as shares
      from {{source('fds_yt','rpt_yt_wwe_engagement_daily')}}
      where trunc(time_uploaded) > Date(getdate()-370)
        and  country_code is not null
      group by 1,2,3,4,5,6,7,8
    )