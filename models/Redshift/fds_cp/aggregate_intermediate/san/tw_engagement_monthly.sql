{{
  config({
		"materialized": 'ephemeral'
  })
}}
select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
		dim_platform_id,
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Engagements' as Metric,
		(sum(likes)+sum(retweets)+sum(replies)) as Value 
from {{source('fds_tw','fact_tw_engagement_post')}}
{% if is_incremental() %}
  where month = date_trunc('month',current_date-28)
{% endif %}
group by 1,2,3,4