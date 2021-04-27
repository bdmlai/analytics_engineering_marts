{{
  config({
		'schema': 'dt_prod_support',
		"materialized": 'table',"tags": 'rpt_cp_weekly_consolidated_kpi'
		  })
}}

select b.*, a.hours_watched as hours_watched_wk, 0 as hours_watched_tier2_wk, 0 as active_network_subscribers_wk, 
0 as hours_per_tot_subscriber_wk, a.views as views_wk, 0 as ad_impressions_wk, 
0 as network_subscriber_adds_wk,
0 as new_adds_wk,
0 as new_adds_direct_t3_wk,
0 as reg_prospects_t2_to_t3_wk,
0 as winback_adds_t2_to_t3_wk,
0 as lp_adds_wk,
0 as new_free_version_regns_wk,
0 as network_losses_wk,
'Snapchat'::varchar(12) as platform,
'Snapchat'::varchar(12) as type
from 
{{ref('intm_dim_dates')}} b
left join 
(       
select monday_date,views,minutes_watched/60 as hours_watched
from {{source('fds_cp','vw_aggr_cp_weekly_consumption_by_platform')}} 
where trunc(monday_date) >= trunc(dateadd('year',-2,date_trunc('year',getdate())))
and platform = 'Snapchat'
)  a
on trunc(a.monday_date) = b.cal_year_mon_week_begin_date