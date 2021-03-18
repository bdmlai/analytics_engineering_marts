
select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')-1) as month,
		1 as dim_platform_id,
		5 as cp_dim_platform,
		dim_smprovider_account_id,
		'Followers' as Metric,
		sum(facebook_followers) as Value 
from "entdwdb"."fds_cp"."fact_co_smfollowership_cumulative_summary"

group by 1,2,3,4