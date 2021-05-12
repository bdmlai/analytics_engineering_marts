{{
  config({
		'schema': 'fds_cp',
		"materialized": 'ephemeral'
  })
}}

(
select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
		dim_platform_id,
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Video Views' as Metric,
		sum(views_3_seconds) as Value 
from {{source('fds_fbk','fact_fb_consumption_parent_video')}}
where month = trunc(date_add('month',-1,date_trunc('month',current_date)))
group by 1,2,3,4)

union all

(
select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
		dim_platform_id,
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Video Views' as Metric,
		sum(video_views) as Value 
from {{source('fds_tw','fact_tw_consumption_post')}}
where month = trunc(date_add('month',-1,date_trunc('month',current_date)))
group by 1,2,3,4)


union all

 (
select  month, 
		dim_platform_id,
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Video Views' as Metric,
		sum(Value) as Value
from
(select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
		dim_platform_id,
		dim_smprovider_account_id,
		sum(video_views) as Value 
from {{source('fds_igm','fact_ig_consumption_post')}}
where month = trunc(date_add('month',-1,date_trunc('month',current_date)))
group by 1,2,3

union all

select 	date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
		dim_platform_id,
		dim_smprovider_account_id,
		sum(impressions) as Value
from 	{{source('fds_igm','fact_ig_consumption_frame')}}
where 	month = trunc(date_add('month',-1,date_trunc('month',current_date)))
group by 1,2,3)
group by 1,2,3,4,5
)