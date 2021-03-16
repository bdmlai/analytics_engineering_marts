

select month,country,platform,sum(views) App_views,sum(hours_watched) as App_hrs_watched 
from "entdwdb"."fds_cp"."vw_rpt_cp_monthly_global_consumption_by_platform" where type='WWE App'
group by month,country,platform