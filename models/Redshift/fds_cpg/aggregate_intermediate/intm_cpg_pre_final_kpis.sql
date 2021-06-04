
{{
  config({
		"schema": 'fds_cpg',
		"materialized": 'ephemeral',"tags": 'rpt_cpg_weekly_consolidated_kpi'
  })
}}

select 
a.granularity, 
a.platform, 
a.type, a.metric, 
a.year, 
a.month, 
a.week, 
a.start_date, 
a.end_date, 
a.value,
a.prev_year, 
a.prev_year_week, 
a.prev_year_start_date, 
a.prev_year_end_date, 
a.prev_year_value
from 
(select * from {{ref("intm_cpg_weekly_pivot")}} union all
select * from {{ref("intm_cpg_monthly_pivot")}} union all
select * from {{ref("intm_cpg_yearly_pivot")}}) a
