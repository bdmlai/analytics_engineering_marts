
{{
  config({
		"schema": 'fds_cpg',
		"materialized": 'ephemeral',"tags": 'rpt_cpg_weekly_consolidated_kpi'
  })
}}


-- create CPG Monthly dataset

select a.platform, 
a.cal_year as year,
a.cal_mth_num as month, 
a.cal_year_week_num_mon as week, 
a.cal_year_mon_week_begin_date as start_date, 
a.cal_year_mon_week_end_date as end_date, 
a.prev_year, 
a.prev_year_week, 
a.prev_year_start_date, 
a.prev_year_end_date, 
sum(b.orders_wk) as orders_mtd,
sum(b.revenue_wk) as revenue_mtd,
avg(b.avg_order_size_wk) as avg_order_size_mtd,
sum(b.prev_orders_wk) as prev_orders_mtd,
sum(b.prev_revenue_wk) as prev_revenue_mtd,
avg(b.prev_avg_order_size_wk) as prev_avg_order_size_mtd
from {{ref("intm_cpg_weekly_kpis")}} a
left join {{ref("intm_cpg_weekly_kpis")}} b
on a.cal_year = b.cal_year and 
a.cal_mth_num = b.cal_mth_num and 
a.cal_year_week_num_mon >= b.cal_year_week_num_mon 
group by 1,2,3,4,5,6,7,8,9,10

