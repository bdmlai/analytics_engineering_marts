
{{
  config({
		"schema": 'fds_nl',
		"materialized": 'ephemeral',"tags": 'rpt_tv_weekly_consolidated_kpi'
  })
}}


select 		* 
from
(
select 'Weekly' as granularity, platform, 'RAW' as type, 'Coverage HH rating' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, Coverage_HH_Rating_Raw as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Coverage_HH_Rating_Raw as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'NXT' as type, 'Coverage HH rating' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, Coverage_HH_Rating_NXT as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Coverage_HH_Rating_NXT as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'RAW' as type, 'National HH Rating ' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, National_HH_Rating_Raw as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_National_HH_Rating_Raw as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'SMD' as type, 'National HH Rating ' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, National_HH_Rating_Smackdown_Fox as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_National_HH_Rating_Smackdown_Fox as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'NXT' as type, 'National HH Rating ' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, National_HH_Rating_NXT as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_National_HH_Rating_NXT as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'Total' as type, 'RAW-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, Raw_Avg_Viewers_Total as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Total as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'Male' as type, 'RAW-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, Raw_Avg_Viewers_Male as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Male as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'Female' as type, 'RAW-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, Raw_Avg_Viewers_Female as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Female as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'Ages 2-17' as type, 'RAW-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, Raw_Avg_Viewers_Ages_2_17 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Ages_2_17 as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'Ages 18-34' as type, 'RAW-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, Raw_Avg_Viewers_Ages_18_34 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Ages_18_34 as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'Ages 35-54' as type, 'RAW-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, Raw_Avg_Viewers_Ages_35_54 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Ages_35_54 as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'Ages 55+' as type, 'RAW-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, Raw_Avg_Viewers_Age_55_plus as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Age_55_plus as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'Ages 18-49' as type, 'RAW-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, Raw_Avg_Viewers_Ages_18_49 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Ages_18_49 as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'Total' as type, 'SMD-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, SD_Avg_Viewers_Total as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Total as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'Male' as type, 'SMD-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, SD_Avg_Viewers_Male as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Male as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'Female' as type, 'SMD-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, SD_Avg_Viewers_Female as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Female as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'Ages 2-17' as type, 'SMD-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, SD_Avg_Viewers_Ages_2_17 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Ages_2_17 as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'Ages 18-34' as type, 'SMD-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, SD_Avg_Viewers_Ages_18_34 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Ages_18_34 as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'Ages 35-54' as type, 'SMD-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, SD_Avg_Viewers_Ages_35_54 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Ages_35_54 as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'Ages 55+' as type, 'SMD-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, SD_Avg_Viewers_Age_55_plus as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Age_55_plus as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'Ages 18-49' as type, 'SMD-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, SD_Avg_Viewers_Ages_18_49 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Ages_18_49 as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'Total' as type, 'NXT-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, NXT_Avg_Viewers_Total as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Total as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'Male' as type, 'NXT-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, NXT_Avg_Viewers_Male as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Male as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'Female' as type, 'NXT-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, NXT_Avg_Viewers_Female as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Female as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'Ages 2-17' as type, 'NXT-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, NXT_Avg_Viewers_Ages_2_17 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Ages_2_17 as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'Ages 18-34' as type, 'NXT-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, NXT_Avg_Viewers_Ages_18_34 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Ages_18_34 as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'Ages 35-54' as type, 'NXT-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, NXT_Avg_Viewers_Ages_35_54 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Ages_35_54 as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'Ages 55+' as type, 'NXT-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, NXT_Avg_Viewers_Age_55_plus as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Age_55_plus as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }} 

union all

select 'Weekly' as granularity, platform, 'Ages 18-49' as type, 'NXT-Avg Viewers' as Metric, cal_year as year, cal_mth_num as month, cal_year_week_num_mon as week, cal_year_mon_week_begin_date as start_date, cal_year_mon_week_end_date as end_date, NXT_Avg_Viewers_Ages_18_49 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Ages_18_49 as prev_year_value from {{ ref("intm_nl_tv_wkly_wd2") }}
)