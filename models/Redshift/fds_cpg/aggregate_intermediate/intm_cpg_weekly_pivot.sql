{{
  config({
		"schema": 'fds_cpg',
		"materialized": 'ephemeral',"tags": 'rpt_cpg_weekly_consolidated_kpi'
  })
}}

--pivot weekly dataset

select * from
(
select 'Weekly' as granularity, 
platform, 
platform as type, 
'Orders' as Metric, 
cal_year as year, 
cal_mth_num as month, 
cal_year_week_num_mon as week, 
cal_year_mon_week_begin_date as start_date, 
cal_year_mon_week_end_date as end_date, 
orders_wk as  value, 
prev_year, 
prev_year_week, 
prev_year_start_date, 
prev_year_end_date, 
prev_orders_wk as prev_year_value 
from {{ref("intm_cpg_weekly_kpis")}} 

union all

select 'Weekly' as granularity, 
platform, 
platform as type, 
'Revenue' as Metric, 
cal_year as year, 
cal_mth_num as month, 
cal_year_week_num_mon as week, 
cal_year_mon_week_begin_date as start_date, 
cal_year_mon_week_end_date as end_date, 
revenue_wk as  value, 
prev_year, 
prev_year_week, 
prev_year_start_date, 
prev_year_end_date, 
prev_revenue_wk as prev_year_value 
from {{ref("intm_cpg_weekly_kpis")}} 

union all

select 'Weekly' as granularity, 
platform, 
platform as type, 
'Avg Order Size' as Metric, 
cal_year as year, 
cal_mth_num as month, 
cal_year_week_num_mon as week, 
cal_year_mon_week_begin_date as start_date, 
cal_year_mon_week_end_date as end_date, 
avg_order_size_wk as  value, 
prev_year, 
prev_year_week, 
prev_year_start_date, 
prev_year_end_date, 
prev_avg_order_size_wk as prev_year_value 
from {{ref("intm_cpg_weekly_kpis")}}
)
