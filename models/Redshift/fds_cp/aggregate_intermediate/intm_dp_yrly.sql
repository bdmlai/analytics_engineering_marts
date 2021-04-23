{{
  config({
		'schema': 'dt_prod_support',
		"materialized": 'table',"tags": 'rpt_cp_weekly_consolidated_kpi'
		  })
}}

select a.platform,a.type,a.cal_year,a.cal_mth_num, a.cal_year_week_num_mon, a.cal_year_mon_week_begin_date, a.cal_year_mon_week_end_date,
a.cal_year||'-'||to_char(a.cal_year_week_num_mon, 'fm00') as week,
a.prev_cal_year, a.prev_cal_year_week_num_mon,
a.prev_cal_year_mon_week_begin_date, a.prev_cal_year_mon_week_end_date,
sum(b.active_network_subscribers_wk) active_network_subscribers_ytd, 
sum(b.hours_watched_wk) hours_watched_ytd, 
sum(b.hours_watched_tier2_wk) hours_watched_tier2_ytd,
sum(b.hours_per_tot_subscriber_wk) hours_per_tot_subscriber_ytd, 
sum(b.views_wk) views_ytd, sum(b.ad_impressions_wk) ad_impressions_ytd,
sum(b.network_subscriber_adds_wk) as network_subscriber_adds_ytd,
sum(b.new_adds_wk) as new_adds_ytd,
sum(b.new_adds_direct_t3_wk) as new_adds_direct_t3_ytd,
sum(b.reg_prospects_t2_to_t3_wk) as reg_prospects_t2_to_t3_ytd,
sum(b.winback_adds_t2_to_t3_wk) as winback_adds_t2_to_t3_ytd,
sum(b.lp_adds_wk) as lp_adds_ytd,
sum(b.new_free_version_regns_wk) as new_free_version_regns_ytd,
sum(b.network_losses_wk) as network_losses_ytd,
sum(b.prev_active_network_subscribers_wk) prev_active_network_subscribers_ytd, 
sum(b.prev_hours_watched_wk) prev_hours_watched_ytd, 
sum(b.prev_hours_watched_tier2_wk) prev_hours_watched_tier2_ytd,
sum(b.prev_hours_per_tot_subscriber_wk) prev_hours_per_tot_subscriber_ytd, 
sum(b.prev_views_wk) prev_views_ytd, sum(b.prev_ad_impressions_wk) prev_ad_impressions_ytd,
sum(b.prev_network_subscriber_adds_wk) as prev_network_subscriber_adds_ytd,
sum(b.prev_new_adds_wk) as prev_new_adds_ytd,
sum(b.prev_new_adds_direct_t3_wk) as prev_new_adds_direct_t3_ytd,
sum(b.prev_reg_prospects_t2_to_t3_wk) as prev_reg_prospects_t2_to_t3_ytd,
sum(b.prev_winback_adds_t2_to_t3_wk) as prev_winback_adds_t2_to_t3_ytd,
sum(b.prev_lp_adds_wk) as prev_lp_adds_ytd,
sum(b.prev_new_free_version_regns_wk) as prev_new_free_version_regns_ytd,
sum(b.prev_network_losses_wk) as prev_network_losses_ytd
from {{ref('intm_dp_wkly1')}} a
left join {{ref('intm_dp_wkly1')}} b
on a.cal_year = b.cal_year and a.cal_year_week_num_mon >= b.cal_year_week_num_mon and a.platform = b.platform and a.type=b.type
group by 1,2,3,4,5,6,7,8,9,10,11,12