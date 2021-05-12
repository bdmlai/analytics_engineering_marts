{{
  config({
		'schema': 'fds_cp',
		"materialized": 'ephemeral'
  })
}}

(
select  month, 
		dim_platform_id,
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Engagement' as Metric,
		sum(Value) as Value
from
(
select  date_trunc('month',to_date(b.dim_date_id,'yyyymmdd')) as month,
		b.dim_platform_id,
		b.dim_smprovider_account_id,
		(sum(b.likes)+sum(b.comments)+sum(b.shares)) as Value
from {{source('fds_fbk','vw_rpt_daily_fb_published_video')}} a
left join  {{source('fds_fbk','fact_fb_engagement_video')}} b
on a.dim_video_id = b.dim_video_id
where a.iscrosspost = false
and month = trunc(date_add('month',-1,date_trunc('month',current_date)))
and a.dim_platform_id = 1
and b.dim_platform_id = 1
group by 1,2,3

union all

select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
		dim_platform_id,
		dim_smprovider_account_id,
		(sum(likes)+sum(comments)+sum(shares)) as Value 
from {{source('fds_fbk','fact_fb_engagement_post')}}
where month = trunc(date_add('month',-1,date_trunc('month',current_date)))
and dim_content_type_id not in ('10236','10003','10230','10234','10257','10260')
group by 1,2,3
)
group by 1,2,3,4,5)

union all

 (
select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
		dim_platform_id,
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Engagement' as Metric,
		(sum(likes)+sum(retweets)+sum(replies)) as Value 
from {{source('fds_tw','fact_tw_engagement_post')}}
where month = trunc(date_add('month',-1,date_trunc('month',current_date)))
group by 1,2,3,4)

union all

 (
select  month, 
		dim_platform_id,
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Engagement' as Metric,
		sum(Value) as Value
from
(
select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
		dim_platform_id,
		dim_smprovider_account_id,
		(sum(likes)+sum(comments)) as Value 
from {{source('fds_igm','fact_ig_engagement_post')}}
where month = trunc(date_add('month',-1,date_trunc('month',current_date)))
group by 1,2,3

union all

select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
		dim_platform_id,
		dim_smprovider_account_id,
		sum(replies) as Value 
from {{source('fds_igm','fact_ig_engagement_frame')}}
where month = trunc(date_add('month',-1,date_trunc('month',current_date)))
group by 1,2,3
)
group by 1,2,3,4)