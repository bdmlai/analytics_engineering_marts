{{
  config({
		'schema': 'dt_prod_support',
		"materialized": 'table',"tags": 'rpt_cp_weekly_consolidated_kpi'
		  })
}}

select a.platform, a.type, a.cal_year, a.cal_mth_num, a.cal_year_week_num_mon, a.cal_year_mon_week_begin_date, a.cal_year_mon_week_end_date, 
a.week, a.prev_cal_year, a.prev_cal_year_week_num_mon, a.prev_cal_year_mon_week_begin_date, a.prev_cal_year_mon_week_end_date,
b.active_network_subscribers_wk as active_network_subscribers_mtd, a.hours_watched_mtd, a.hours_watched_tier2_mtd, 
a.hours_watched_mtd/nullif(b.active_network_subscribers_wk,0) as hours_per_tot_subscriber_mtd, 
a.views_mtd, a.ad_impressions_mtd, a.network_subscriber_adds_mtd, a.new_adds_mtd, a.new_adds_direct_t3_mtd, a.reg_prospects_t2_to_t3_mtd,
a.winback_adds_t2_to_t3_mtd, a.lp_adds_mtd, a.new_free_version_regns_mtd, a.network_losses_mtd, 
b.prev_active_network_subscribers_wk as prev_active_network_subscribers_mtd, a.prev_hours_watched_mtd, a.prev_hours_watched_tier2_mtd,
a.prev_hours_watched_mtd/nullif(b.prev_active_network_subscribers_wk,0) as prev_hours_per_tot_subscriber_mtd, 
a.prev_views_mtd, a.prev_ad_impressions_mtd, a.prev_network_subscriber_adds_mtd, a.prev_new_adds_mtd, a.prev_new_adds_direct_t3_mtd, a.prev_reg_prospects_t2_to_t3_mtd,
a.prev_winback_adds_t2_to_t3_mtd, a.prev_lp_adds_mtd, a.prev_new_free_version_regns_mtd, a.prev_network_losses_mtd
from {{ref('intm_dp_mthly')}} a
left join {{ref('intm_dp_wkly1')}} b
on a.platform = b.platform
and a.type = b.type
and a.cal_year = b.cal_year
and a.cal_mth_num = b.cal_mth_num
and a.cal_year_week_num_mon = b.cal_year_week_num_mon
and a.cal_year_mon_week_begin_date = b.cal_year_mon_week_begin_date
and a.cal_year_mon_week_end_date = b.cal_year_mon_week_end_date
and a.week = b.week
where a.platform = 'Network' and b.platform = 'Network'