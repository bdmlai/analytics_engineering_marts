{{
  config({
		'schema': 'fds_cp',
		"materialized": 'ephemeral'
  })
}}

(
select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')-1) as month,
		1 as dim_platform_id,
		5 as cp_dim_platform,
		dim_smprovider_account_id,
		'Followers' as Metric,
		sum(facebook_followers) as Value 
from {{source('fds_cp','fact_co_smfollowership_cumulative_summary')}}
where to_date(dim_date_id,'yyyymmdd') = date_trunc('month',current_date)
group by 1,2,3,4)

union all
 (
select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')-1) as month,
		4 as dim_platform_id,
		5 as cp_dim_platform,
		dim_smprovider_account_id,
		'Followers' as Metric,
		sum(twitter_followers) as Value 
from {{source('fds_cp','fact_co_smfollowership_cumulative_summary')}}
where to_date(dim_date_id,'yyyymmdd') = date_trunc('month',current_date)
group by 1,2,3,4)


union all

(
select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')-1) as month,
		2 as dim_platform_id,
		5 as cp_dim_platform,
		dim_smprovider_account_id,
		'Followers' as Metric,
		sum(instagram_followers) as Value 
from {{source('fds_cp','fact_co_smfollowership_cumulative_summary')}}
where to_date(dim_date_id,'yyyymmdd') = date_trunc('month',current_date)
group by 1,2,3,4)