{{
  config({
		"schema": 'fds_yt',
		"materialized": 'table','tags': "Content", "persist_docs": {'relation' : true, 'columns' : true},
	        'post-hook': 'grant select on fds_yt.rpt_yt_daily_episodes_by_brand to public'
                
  })
}}

select channel_id
	,channel_name
	,video_id
	,title
	,country_code
	,time_uploaded
	,report_date_dt
	,views,comments
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
	,'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_content' as etl_batch_id
	,'bi_dbt_user_prd' as etl_insert_user_id
	, current_timestamp as etl_insert_rec_dttm
	, null as etl_update_user_id
	, cast(null as timestamp) as etl_update_rec_dttm


from
	(
	  select * from {{ref('intm_yt_daily_engagement_amg_content_grp')}} 
	        union all 
	  select * from {{ref('intm_yt_daily_engagement_weeklyshowclips_latest')}}
		union all 
	  select * from {{ref('intm_yt_daily_engagement_weeklyshowclips_previous')}}
	)