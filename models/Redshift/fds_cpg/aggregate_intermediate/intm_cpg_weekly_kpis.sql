{{
  config({
		"schema": 'fds_cpg',
		"materialized": 'ephemeral',"tags": 'rpt_cpg_weekly_consolidated_kpi'
  })
}}


select a.*, 
b.cal_year as prev_year, 
b.cal_year_week_num_mon as prev_year_week,
b.cal_year_mon_week_begin_date as prev_year_start_date, 
b.cal_year_mon_week_end_date as prev_year_end_date,
coalesce(b.orders_wk,0) prev_orders_wk,
coalesce(b.revenue_wk,0) prev_revenue_wk,
coalesce(b.avg_order_size_wk,0) prev_avg_order_size_wk
from {{ref("intm_cpg_weekly")}} a
left join
{{ref("intm_cpg_weekly")}} b
on (a.cal_year-1) = b.cal_year and 
a.cal_year_week_num_mon = b.cal_year_week_num_mon and 
a.platform = b.platform
