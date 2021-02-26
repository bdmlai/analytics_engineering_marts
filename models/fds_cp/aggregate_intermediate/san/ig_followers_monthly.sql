{{
  config({
		"materialized": 'ephemeral'
  })
}}
select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')-1) as month,
		2 as dim_platform_id,
		5 as cp_dim_platform,
		dim_smprovider_account_id,
		'Followers' as Metric,
		sum(instagram_followers) as Value 
from {{source('fds_cp','fact_co_smfollowership_cumulative_summary')}}
{% if is_incremental() %}
  where to_date(dim_date_id,'yyyymmdd') = date_trunc('month',current_date)
{% endif %}

group by 1,2,3,4