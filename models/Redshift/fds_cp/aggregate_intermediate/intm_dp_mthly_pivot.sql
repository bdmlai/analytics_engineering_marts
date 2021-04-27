{{
  config({
		'schema': 'dt_prod_support',
		"materialized": 'table',"tags": 'rpt_cp_weekly_consolidated_kpi'
		  })
}}

select * from
(
select 'MTD' as granularity, platform, type, 'Hours Watched' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, hours_watched_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_hours_watched_mtd as prev_year_value
from {{ref('intm_dp_mthly')}}
union all
select 'MTD' as granularity, platform, type, 'Hours Watched Tier2' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, hours_watched_tier2_mtd as  value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_hours_watched_tier2_mtd as prev_year_value
from {{ref('intm_dp_mthly')}} where platform = 'Network'
union all
select 'MTD' as granularity, platform, type, case when platform ='Network' then 'Active Viewers' else 'Views' end as Metric, 
week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, views_mtd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_views_mtd as prev_year_value
from {{ref('intm_dp_mthly')}}
union all
select 'MTD' as granularity, platform, type, 'Ad Impressions' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, ad_impressions_mtd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_ad_impressions_mtd as prev_year_value
from {{ref('intm_dp_mthly')}}  where platform = 'Youtube'
union all
select 'MTD' as granularity, platform, type, 'Active Network Subscribers' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, active_network_subscribers_wk as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_active_network_subscribers_wk as prev_year_value
from {{ref('intm_dp_wkly1')}} where platform = 'Network'
union all
select 'MTD' as granularity, platform, type, 'Hours per total Subscriber' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, hours_per_tot_subscriber_mtd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_hours_per_tot_subscriber_mtd as prev_year_value
from {{ref('intm_dp_mthly1')}} where platform = 'Network'
union all
select 'MTD' as granularity, platform, type, 'Network Subscriber Adds' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, network_subscriber_adds_mtd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_network_subscriber_adds_mtd as prev_year_value
from {{ref('intm_dp_mthly')}} where platform = 'Network'
union all
select 'MTD' as granularity, platform, type, 'New Adds' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, new_adds_mtd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_new_adds_mtd as prev_year_value
from {{ref('intm_dp_mthly')}} where platform = 'Network'
union all
select 'MTD' as granularity, platform, type, 'New Adds (Direct to T3)' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, new_adds_direct_t3_mtd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_new_adds_direct_t3_mtd as prev_year_value
from {{ref('intm_dp_mthly')}} where platform = 'Network'
union all
select 'MTD' as granularity, platform, type, 'Registered Prospects (T2 to T3)' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, reg_prospects_t2_to_t3_mtd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_reg_prospects_t2_to_t3_mtd as prev_year_value
from {{ref('intm_dp_mthly')}} where platform = 'Network'
union all
select 'MTD' as granularity, platform, type, 'Winback Adds (T2 to T3)' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, winback_adds_t2_to_t3_mtd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_winback_adds_t2_to_t3_mtd as prev_year_value
from {{ref('intm_dp_mthly')}} where platform = 'Network'
union all
select 'MTD' as granularity, platform, type, 'License Partner Adds (direct to T3)' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, lp_adds_mtd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_lp_adds_mtd as prev_year_value
from {{ref('intm_dp_mthly')}} where platform = 'Network'
union all
select 'MTD' as granularity, platform, type, 'New Free Version Registrations (New to T2)' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, new_free_version_regns_mtd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_new_free_version_regns_mtd as prev_year_value
from {{ref('intm_dp_mthly')}} where platform = 'Network'
union all
select 'MTD' as granularity, platform, type, 'Network Losses (T3 to T2)' as Metric, week, cal_year, cal_mth_num, cal_year_week_num_mon, 
cal_year_mon_week_begin_date, cal_year_mon_week_end_date, network_losses_mtd as value,
prev_cal_year, prev_cal_year_week_num_mon, prev_cal_year_mon_week_begin_date, prev_cal_year_mon_week_end_date, prev_network_losses_mtd as prev_year_value
from {{ref('intm_dp_mthly')}} where platform = 'Network'
)