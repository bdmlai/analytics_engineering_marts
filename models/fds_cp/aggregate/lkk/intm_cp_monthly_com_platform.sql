{{
  config({
		"materialized": 'ephemeral'
  })
}}
select month,country,platform,sum(views) Com_views,sum(hours_watched) as Com_hrs_watched 
from fds_cp.vw_rpt_cp_monthly_global_consumption_by_platform where type='WWE.COM'
group by month,country,platform