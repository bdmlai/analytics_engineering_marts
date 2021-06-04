{{
  config({
		"schema": 'fds_cpg',
		"materialized": 'ephemeral',"tags": 'rpt_cpg_weekly_consolidated_kpi'
  })
}}

--pivot monthly dataset


select * from
(
select 'MTD' as granularity, 
platform, 
platform as type, 
'Orders' as Metric, 
year, 
month, 
week, 
start_date, 
end_date, 
orders_mtd as  value, 
prev_year, 
prev_year_week, 
prev_year_start_date, 
prev_year_end_date, 
prev_orders_mtd as prev_year_value 
from {{ref("intm_cpg_monthly_kpis")}} 

union all


select 'MTD' as granularity, 
platform, 
platform as type, 
'Revenue' as Metric, 
year, 
month, 
week, 
start_date, 
end_date, 
revenue_mtd as  value, 
prev_year, 
prev_year_week, 
prev_year_start_date, 
prev_year_end_date,
prev_revenue_mtd as prev_year_value 
from {{ref("intm_cpg_monthly_kpis")}} 

union all


select 'MTD' as granularity, 
platform, 
platform as type, 
'Avg Order Size' as Metric, 
year, 
month, 
week, 
start_date, 
end_date, 
avg_order_size_mtd as  value, 
prev_year, 
prev_year_week, 
prev_year_start_date, 
prev_year_end_date, 
prev_avg_order_size_mtd as prev_year_value 
from {{ref("intm_cpg_monthly_kpis")}}
)
