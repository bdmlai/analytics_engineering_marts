{{
  config({
		'schema': 'fds_cp',
		"materialized": 'ephemeral'
  })
}}
 
(
select  month, 
		dim_platform_id,
		cp_dim_platform,
		dim_smprovider_account_id,
		'Post Volume' as Metric,
		sum(Value) as Value
from
((
select 	date_trunc('month',date_posted) as month,
		dim_platform_id,
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Post Volume' as Metric,
		count(distinct dim_video_id) as Value
from 	{{source('fds_fbk','vw_rpt_daily_fb_published_video')}}
where 	iscrosspost ='false' 
		and month = trunc(date_add('month',-1,date_trunc('month',current_date)))
group by 1,2,3,4)
union all
(
select 	date_trunc('month',post_date) as month,
		dim_platform_id, 
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Post Volume' as Metric,
		count(distinct dim_media_id) as Value
from 	{{source('fds_fbk','vw_rpt_daily_fb_published_post')}}
where 	month = trunc(date_add('month',-1,date_trunc('month',current_date)))
		and dim_content_type_id not in ('10236','10003','10230','10234','10257','10260')
group by 1,2,3,4))
group by 1,2,3,4,5)

union all 

(
select 	date_trunc('month',date_posted) as month,
		dim_platform_id,
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Post Volume' as Metric,
		(count (distinct dim_video_id) + count (distinct dim_media_id)) as Value
from 	{{source('fds_tw','vw_rpt_daily_tw_published_post')}}
where 	month = trunc(date_add('month',-1,date_trunc('month',current_date)))
group by 1,2,3,4)

union all

(
select  month, 
		dim_platform_id,
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Post Volume' as Metric,
		sum(Value) as Value
from
(select date_trunc('month',post_date) as month,
		dim_platform_id,
		dim_smprovider_account_id,
		(count (distinct dim_video_id) + count (distinct dim_media_id)) as Value
from 	{{source('fds_igm','vw_rpt_daily_ig_published_post')}}
where 	month = trunc(date_add('month',-1,date_trunc('month',current_date)))
group by 1,2,3

union all

select 	date_trunc('month',frame_date) as month,
		dim_platform_id,
		dim_smprovider_account_id,
		(count (distinct dim_video_id) + count (distinct dim_media_id)) as Value
from  {{source('fds_igm','vw_rpt_daily_ig_published_frame')}}
where 	month = trunc(date_add('month',-1,date_trunc('month',current_date)))
group by 1,2,3)
group by 1,2,3,4,5)