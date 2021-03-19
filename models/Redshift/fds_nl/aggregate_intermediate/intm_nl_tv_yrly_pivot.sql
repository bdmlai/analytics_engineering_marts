{{
  config({
		"schema": 'fds_nl',
		"materialized": 'ephemeral'
  })
}}


with tv_yrly as
(
select 		a.platform, 
			a.cal_year as year,
			a.cal_mth_num as month, 
			a.cal_year_week_num_mon as week, 
			a.cal_year_mon_week_begin_date as start_date, 
			a.cal_year_mon_week_end_date as end_date, 
			a.prev_year, 
			a.prev_year_week, 
			a.prev_year_start_date, 
			a.prev_year_end_date,
			sum(b.Coverage_HH_Rating_Raw*b.raw_duration)/sum(b.raw_duration) as Coverage_HH_Rating_Raw,
			sum(b.Coverage_HH_Rating_NXT*b.nxt_duration)/sum(b.nxt_duration) as Coverage_HH_Rating_NXT,
			sum(b.National_HH_Rating_Raw*b.raw_duration)/sum(b.raw_duration) as National_HH_Rating_Raw,
			sum(b.National_HH_Rating_Smackdown_Fox*b.smd_duration)/sum(b.smd_duration) as National_HH_Rating_Smackdown_Fox,
			sum(b.National_HH_Rating_NXT*b.nxt_duration)/sum(b.nxt_duration) as National_HH_Rating_NXT,
			sum(b.Raw_Avg_Viewers_Total*b.raw_duration)/sum(b.raw_duration) as Raw_Avg_Viewers_Total,
			sum(b.Raw_Avg_Viewers_Male*b.raw_duration)/sum(b.raw_duration) as Raw_Avg_Viewers_Male,
			sum(b.Raw_Avg_Viewers_Female*b.raw_duration)/sum(b.raw_duration) as Raw_Avg_Viewers_Female,
			sum(b.Raw_Avg_Viewers_Ages_2_17*b.raw_duration)/sum(b.raw_duration) as Raw_Avg_Viewers_Ages_2_17,
			sum(b.Raw_Avg_Viewers_Ages_18_34*b.raw_duration)/sum(b.raw_duration) as Raw_Avg_Viewers_Ages_18_34,
			sum(b.Raw_Avg_Viewers_Ages_35_54*b.raw_duration)/sum(b.raw_duration) as Raw_Avg_Viewers_Ages_35_54,
			sum(b.Raw_Avg_Viewers_Age_55_plus*b.raw_duration)/sum(b.raw_duration) as Raw_Avg_Viewers_Age_55_plus,
			sum(b.Raw_Avg_Viewers_Ages_18_49*b.raw_duration)/sum(b.raw_duration) as Raw_Avg_Viewers_Ages_18_49,
			sum(b.SD_Avg_Viewers_Total*b.smd_duration)/sum(b.smd_duration) as SD_Avg_Viewers_Total,
			sum(b.SD_Avg_Viewers_Male*b.smd_duration)/sum(b.smd_duration) as SD_Avg_Viewers_Male,
			sum(b.SD_Avg_Viewers_Female*b.smd_duration)/sum(b.smd_duration) as SD_Avg_Viewers_Female,
			sum(b.SD_Avg_Viewers_Ages_2_17*b.smd_duration)/sum(b.smd_duration) as SD_Avg_Viewers_Ages_2_17,
			sum(b.SD_Avg_Viewers_Ages_18_34*b.smd_duration)/sum(b.smd_duration) as SD_Avg_Viewers_Ages_18_34,
			sum(b.SD_Avg_Viewers_Ages_35_54*b.smd_duration)/sum(b.smd_duration) as SD_Avg_Viewers_Ages_35_54,
			sum(b.SD_Avg_Viewers_Age_55_plus*b.smd_duration)/sum(b.smd_duration) as SD_Avg_Viewers_Age_55_plus,
			sum(b.SD_Avg_Viewers_Ages_18_49*b.smd_duration)/sum(b.smd_duration) as SD_Avg_Viewers_Ages_18_49,
			sum(b.NXT_Avg_Viewers_Total*b.nxt_duration)/sum(b.nxt_duration) as NXT_Avg_Viewers_Total,
			sum(b.NXT_Avg_Viewers_Male*b.nxt_duration)/sum(b.nxt_duration) as NXT_Avg_Viewers_Male,
			sum(b.NXT_Avg_Viewers_Female*b.nxt_duration)/sum(b.nxt_duration) as NXT_Avg_Viewers_Female,
			sum(b.NXT_Avg_Viewers_Ages_2_17*b.nxt_duration)/sum(b.nxt_duration) as NXT_Avg_Viewers_Ages_2_17,
			sum(b.NXT_Avg_Viewers_Ages_18_34*b.nxt_duration)/sum(b.nxt_duration) as NXT_Avg_Viewers_Ages_18_34,
			sum(b.NXT_Avg_Viewers_Ages_35_54*b.nxt_duration)/sum(b.nxt_duration) as NXT_Avg_Viewers_Ages_35_54,
			sum(b.NXT_Avg_Viewers_Age_55_plus*b.nxt_duration)/sum(b.nxt_duration) as NXT_Avg_Viewers_Age_55_plus,
			sum(b.NXT_Avg_Viewers_Ages_18_49*b.nxt_duration)/sum(b.nxt_duration) as NXT_Avg_Viewers_Ages_18_49,
			sum(b.prev_Coverage_HH_Rating_Raw*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Coverage_HH_Rating_Raw,
			sum(b.prev_Coverage_HH_Rating_NXT*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_Coverage_HH_Rating_NXT,
			sum(b.prev_National_HH_Rating_Raw*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_National_HH_Rating_Raw,
			sum(b.prev_National_HH_Rating_Smackdown_Fox*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_National_HH_Rating_Smackdown_Fox,
			sum(b.prev_National_HH_Rating_NXT*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_National_HH_Rating_NXT,
			sum(b.prev_Raw_Avg_Viewers_Total*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Raw_Avg_Viewers_Total,
			sum(b.prev_Raw_Avg_Viewers_Male*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Raw_Avg_Viewers_Male,
			sum(b.prev_Raw_Avg_Viewers_Female*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Raw_Avg_Viewers_Female,
			sum(b.prev_Raw_Avg_Viewers_Ages_2_17*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Raw_Avg_Viewers_Ages_2_17,
			sum(b.prev_Raw_Avg_Viewers_Ages_18_34*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Raw_Avg_Viewers_Ages_18_34,
			sum(b.prev_Raw_Avg_Viewers_Ages_35_54*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Raw_Avg_Viewers_Ages_35_54,
			sum(b.prev_Raw_Avg_Viewers_Age_55_plus*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Raw_Avg_Viewers_Age_55_plus,
			sum(b.prev_Raw_Avg_Viewers_Ages_18_49*b.prev_raw_duration)/nullif(sum(b.prev_raw_duration),0) as prev_Raw_Avg_Viewers_Ages_18_49,
			sum(b.prev_SD_Avg_Viewers_Total*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_SD_Avg_Viewers_Total,
			sum(b.prev_SD_Avg_Viewers_Male*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_SD_Avg_Viewers_Male,
			sum(b.prev_SD_Avg_Viewers_Female*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_SD_Avg_Viewers_Female,
			sum(b.prev_SD_Avg_Viewers_Ages_2_17*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_SD_Avg_Viewers_Ages_2_17,
			sum(b.prev_SD_Avg_Viewers_Ages_18_34*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_SD_Avg_Viewers_Ages_18_34,
			sum(b.prev_SD_Avg_Viewers_Ages_35_54*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_SD_Avg_Viewers_Ages_35_54,
			sum(b.prev_SD_Avg_Viewers_Age_55_plus*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_SD_Avg_Viewers_Age_55_plus,
			sum(b.prev_SD_Avg_Viewers_Ages_18_49*b.prev_smd_duration)/nullif(sum(b.prev_smd_duration),0) as prev_SD_Avg_Viewers_Ages_18_49,
			sum(b.prev_NXT_Avg_Viewers_Total*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_NXT_Avg_Viewers_Total,
			sum(b.prev_NXT_Avg_Viewers_Male*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_NXT_Avg_Viewers_Male,
			sum(b.prev_NXT_Avg_Viewers_Female*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_NXT_Avg_Viewers_Female,
			sum(b.prev_NXT_Avg_Viewers_Ages_2_17*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_NXT_Avg_Viewers_Ages_2_17,
			sum(b.prev_NXT_Avg_Viewers_Ages_18_34*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_NXT_Avg_Viewers_Ages_18_34,
			sum(b.prev_NXT_Avg_Viewers_Ages_35_54*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_NXT_Avg_Viewers_Ages_35_54,
			sum(b.prev_NXT_Avg_Viewers_Age_55_plus*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_NXT_Avg_Viewers_Age_55_plus,
			sum(b.prev_NXT_Avg_Viewers_Ages_18_49*b.prev_nxt_duration)/nullif(sum(b.prev_nxt_duration),0) as prev_NXT_Avg_Viewers_Ages_18_49

from 		{{ ref("intm_nl_tv_wkly_wd2") }} a

left join 	{{ ref("intm_nl_tv_wkly_wd2") }} b on a.cal_year = b.cal_year and a.cal_year_week_num_mon >= b.cal_year_week_num_mon 

group by 	1,2,3,4,5,6,7,8,9,10
)


select 		* 

from
(
select 'YTD' as granularity, platform, 'RAW' as type, 'Coverage HH rating' as Metric, year, month, week, start_date, end_date, Coverage_HH_Rating_Raw as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Coverage_HH_Rating_Raw as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'NXT' as type, 'Coverage HH rating' as Metric, year, month, week, start_date, end_date, Coverage_HH_Rating_NXT as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Coverage_HH_Rating_NXT as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'RAW' as type, 'National HH Rating ' as Metric, year, month, week, start_date, end_date, National_HH_Rating_Raw as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_National_HH_Rating_Raw as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'SMD' as type, 'National HH Rating ' as Metric, year, month, week, start_date, end_date, National_HH_Rating_Smackdown_Fox as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_National_HH_Rating_Smackdown_Fox as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'NXT' as type, 'National HH Rating ' as Metric, year, month, week, start_date, end_date, National_HH_Rating_NXT as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_National_HH_Rating_NXT as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'Total' as type, 'RAW-Avg Viewers' as Metric, year, month, week, start_date, end_date, Raw_Avg_Viewers_Total as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Total as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'Male' as type, 'RAW-Avg Viewers' as Metric, year, month, week, start_date, end_date, Raw_Avg_Viewers_Male as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Male as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'Female' as type, 'RAW-Avg Viewers' as Metric, year, month, week, start_date, end_date, Raw_Avg_Viewers_Female as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Female as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'Ages 2-17' as type, 'RAW-Avg Viewers' as Metric, year, month, week, start_date, end_date, Raw_Avg_Viewers_Ages_2_17 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Ages_2_17 as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'Ages 18-34' as type, 'RAW-Avg Viewers' as Metric, year, month, week, start_date, end_date, Raw_Avg_Viewers_Ages_18_34 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Ages_18_34 as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'Ages 35-54' as type, 'RAW-Avg Viewers' as Metric, year, month, week, start_date, end_date, Raw_Avg_Viewers_Ages_35_54 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Ages_35_54 as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'Ages 55+' as type, 'RAW-Avg Viewers' as Metric, year, month, week, start_date, end_date, Raw_Avg_Viewers_Age_55_plus as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Age_55_plus as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'Ages 18-49' as type, 'RAW-Avg Viewers' as Metric, year, month, week, start_date, end_date, Raw_Avg_Viewers_Ages_18_49 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_Raw_Avg_Viewers_Ages_18_49 as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'Total' as type, 'SMD-Avg Viewers' as Metric, year, month, week, start_date, end_date, SD_Avg_Viewers_Total as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Total as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'Male' as type, 'SMD-Avg Viewers' as Metric, year, month, week, start_date, end_date, SD_Avg_Viewers_Male as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Male as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'Female' as type, 'SMD-Avg Viewers' as Metric, year, month, week, start_date, end_date, SD_Avg_Viewers_Female as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Female as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'Ages 2-17' as type, 'SMD-Avg Viewers' as Metric, year, month, week, start_date, end_date, SD_Avg_Viewers_Ages_2_17 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Ages_2_17 as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'Ages 18-34' as type, 'SMD-Avg Viewers' as Metric, year, month, week, start_date, end_date, SD_Avg_Viewers_Ages_18_34 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Ages_18_34 as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'Ages 35-54' as type, 'SMD-Avg Viewers' as Metric, year, month, week, start_date, end_date, SD_Avg_Viewers_Ages_35_54 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Ages_35_54 as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'Ages 55+' as type, 'SMD-Avg Viewers' as Metric, year, month, week, start_date, end_date, SD_Avg_Viewers_Age_55_plus as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Age_55_plus as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'Ages 18-49' as type, 'SMD-Avg Viewers' as Metric, year, month, week, start_date, end_date, SD_Avg_Viewers_Ages_18_49 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_SD_Avg_Viewers_Ages_18_49 as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'Total' as type, 'NXT-Avg Viewers' as Metric, year, month, week, start_date, end_date, NXT_Avg_Viewers_Total as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Total as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'Male' as type, 'NXT-Avg Viewers' as Metric, year, month, week, start_date, end_date, NXT_Avg_Viewers_Male as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Male as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'Female' as type, 'NXT-Avg Viewers' as Metric, year, month, week, start_date, end_date, NXT_Avg_Viewers_Female as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Female as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'Ages 2-17' as type, 'NXT-Avg Viewers' as Metric, year, month, week, start_date, end_date, NXT_Avg_Viewers_Ages_2_17 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Ages_2_17 as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'Ages 18-34' as type, 'NXT-Avg Viewers' as Metric, year, month, week, start_date, end_date, NXT_Avg_Viewers_Ages_18_34 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Ages_18_34 as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'Ages 35-54' as type, 'NXT-Avg Viewers' as Metric, year, month, week, start_date, end_date, NXT_Avg_Viewers_Ages_35_54 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Ages_35_54 as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'Ages 55+' as type, 'NXT-Avg Viewers' as Metric, year, month, week, start_date, end_date, NXT_Avg_Viewers_Age_55_plus as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Age_55_plus as prev_year_value from tv_yrly 

union all

select 'YTD' as granularity, platform, 'Ages 18-49' as type, 'NXT-Avg Viewers' as Metric, year, month, week, start_date, end_date, NXT_Avg_Viewers_Ages_18_49 as  value, prev_year, prev_year_week, prev_year_start_date, prev_year_end_date, prev_NXT_Avg_Viewers_Ages_18_49 as prev_year_value from tv_yrly
)