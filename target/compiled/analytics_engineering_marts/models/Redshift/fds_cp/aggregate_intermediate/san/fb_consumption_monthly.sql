
select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
		dim_platform_id,
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Video Views' as Metric,
		sum(views_3_seconds) as Value 
from "entdwdb"."fds_fbk"."fact_fb_consumption_parent_video"

group by 1,2,3,4