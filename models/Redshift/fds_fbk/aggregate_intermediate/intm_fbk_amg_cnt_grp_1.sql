{{
  config({
		"materialized": 'ephemeral'
  })
}}
select a.* , b.dim_smprovider_account_id 
from  {{ref('intm_fbk_amg_cnt_grp')}} as a 
left join 
	fds_fbk.vw_aggr_fb_daily_consumption_engagement_by_video_todate as b 
on a.dim_video_id = b.dim_video_id