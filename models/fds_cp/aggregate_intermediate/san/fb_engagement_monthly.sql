{{
  config({
		"materialized": 'ephemeral'
  })
}}

select  month, 
		dim_platform_id,
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Engagements' as Metric,
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

{% if is_incremental() %}
  and month = date_trunc('month',current_date-28)
{% endif %}
and a.dim_platform_id = 1
and b.dim_platform_id = 1
group by 1,2,3

union all

select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
		dim_platform_id,
		dim_smprovider_account_id,
		(sum(likes)+sum(comments)+sum(shares)) as Value 
from {{source('fds_fbk','fact_fb_engagement_post')}}
dim_content_type_id not in ('10236','10003','10230','10234','10257','10260')

{% if is_incremental() %}
  and month = date_trunc('month',current_date-28)
{% endif %}


group by 1,2,3
)
group by 1,2,3,4,5