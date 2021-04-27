{{
  config({
		'schema': 'dt_prod_support',
		"materialized": 'table',"tags": 'rpt_cp_weekly_consolidated_kpi'
		  })
}}

select distinct platform, type,cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, hours_watched_wk::decimal(15,1), hours_watched_tier2_wk::decimal(15,1), views_wk::decimal(15,1),
active_network_subscribers_wk::decimal(15,1), hours_per_tot_subscriber_wk::decimal(15,1), ad_impressions_wk::decimal(15,1),
network_subscriber_adds_wk::decimal(15,1),
new_adds_wk::decimal(15,1),
new_adds_direct_t3_wk::decimal(15,1),
reg_prospects_t2_to_t3_wk::decimal(15,1),
winback_adds_t2_to_t3_wk::decimal(15,1),
lp_adds_wk::decimal(15,1),
new_free_version_regns_wk::decimal(15,1),
network_losses_wk::decimal(15,1)
from (
select * from {{ref('intm_dp_wkly_nw_glbl')}} union all
select * from {{ref('intm_dp_wkly_nw_intl')}} union all
select * from {{ref('intm_dp_wkly_fb')}} union all
select * from {{ref('intm_dp_wkly_dc')}} union all
select * from {{ref('intm_dp_wkly_tw')}} union all
select * from {{ref('intm_dp_wkly_ig')}} union all
select * from {{ref('intm_dp_wkly_sc')}} union all
select * from {{ref('intm_dp_wkly_yt')}} union all
select * from {{ref('intm_dp_wkly_tt')}})