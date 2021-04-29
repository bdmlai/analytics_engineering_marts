{{
  config({
		'schema': 'dt_prod_support',
		"materialized": 'table',"tags": 'rpt_cp_weekly_consolidated_kpi'
		  })
}}

select a.*, a.cal_year||'-'||to_char(a.cal_year_week_num_mon, 'fm00') as week,
b.cal_year as prev_cal_year, b.cal_year_week_num_mon as prev_cal_year_week_num_mon,
b.cal_year_mon_week_begin_date as prev_cal_year_mon_week_begin_date, b.cal_year_mon_week_end_date as prev_cal_year_mon_week_end_date,
coalesce(b.active_network_subscribers_wk,0) as prev_active_network_subscribers_wk, coalesce(b.hours_watched_wk,0) as prev_hours_watched_wk,
coalesce(b.hours_watched_tier2_wk,0) as prev_hours_watched_tier2_wk,
coalesce(b.hours_per_tot_subscriber_wk,0) as prev_hours_per_tot_subscriber_wk,coalesce(b.views_wk,0) as prev_views_wk,
coalesce(b.ad_impressions_wk,0) as prev_ad_impressions_wk,
coalesce(b.network_subscriber_adds_wk,0) as prev_network_subscriber_adds_wk,
coalesce(b.new_adds_wk,0) as prev_new_adds_wk,
coalesce(b.new_adds_direct_t3_wk,0) as prev_new_adds_direct_t3_wk,
coalesce(b.reg_prospects_t2_to_t3_wk,0) as prev_reg_prospects_t2_to_t3_wk,
coalesce(b.winback_adds_t2_to_t3_wk,0) as prev_winback_adds_t2_to_t3_wk,
coalesce(b.lp_adds_wk,0) as prev_lp_adds_wk,
coalesce(b.new_free_version_regns_wk,0) as prev_new_free_version_regns_wk,
coalesce(b.network_losses_wk,0) as prev_network_losses_wk
from 
{{ref('intm_dp_wkly')}} a
left join 
{{ref('intm_dp_wkly')}} b
on (a.cal_year-1) = b.cal_year and a.cal_year_week_num_mon = b.cal_year_week_num_mon 
and a.platform = b.platform and a.type=b.type