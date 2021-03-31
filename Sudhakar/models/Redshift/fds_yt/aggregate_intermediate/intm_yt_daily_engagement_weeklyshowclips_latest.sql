{{
  config({
		"materialized": 'ephemeral'
        })
}}

select * from
	(
	   select channel_id,channel_name
	         ,video_id,title,country_code
		 ,time_uploaded,a.report_date_dt
		 ,views,comments,likes,dislikes
		 ,shares,a.brand,playlist_title
		 ,day_uploaded,episode_air_date
		 ,day_flag,"date flag"
		 ,week_uploaded,playlist_flag
		 ,brand_dow,"pl flag"
		 ,'recent' as "Table Type" 
	  from  
	   ( select * from {{ref('intm_yt_daily_engagement_amg_content_grp')}}) a 
  	  inner join  
	   ( select brand,max(report_date_dt) as report_date_dt 
		from  {{ref('intm_yt_daily_engagement_amg_content_grp')}} group by 1) b 
   	        on  a.brand =b.brand 
		and a.report_date_dt = b.report_date_dt
	) 
where "Pl Flag" ='Latest week'