{{
  config({
		"schema": 'fds_cpg',
		"materialized": 'ephemeral',"tags": 'rpt_cpg_weekly_consolidated_kpi'
  })
}}
--pivot yearly dataset


select * from
(
select 'YTD' as granularity, 
platform, 
platform as type, 
'Orders' as Metric, 
year, 
month, 
week, 
start_date, 
end_date, 
orders_ytd as  value, 
prev_year, 
prev_year_week, 
prev_year_start_date, 
prev_year_end_date, 
prev_orders_ytd as prev_year_value 
from {{ref("intm_cpg_yearly_kpis")}} 

union all

select 'YTD' as granularity, 
platform, 
platform as type, 
'Revenue' as Metric, 
year, 
month, 
week, 
start_date, 
end_date, 
revenue_ytd as  value, 
prev_year, 
prev_year_week, 
prev_year_start_date, 
prev_year_end_date, 
prev_revenue_ytd as prev_year_value 
from {{ref("intm_cpg_yearly_kpis")}} 

union all

select 'YTD' as granularity, 
platform, 
platform as type, 
'Avg Order Size' as Metric, 
year, 
month, 
week, 
start_date, 
end_date, 
avg_order_size_ytd as  value, 
prev_year, 
prev_year_week, 
prev_year_start_date, 
prev_year_end_date, 
prev_avg_order_size_ytd as prev_year_value 
from {{ref("intm_cpg_yearly_kpis")}}
)
