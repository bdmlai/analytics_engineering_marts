 
{{
  config({
		"schema": 'fds_cpg',
		"materialized": 'ephemeral',"tags": 'rpt_cpg_weekly_consolidated_kpi'
  })
}}

select extract('year' from cal_year_mon_week_begin_date) as cal_year, 
extract('month' from cal_year_mon_week_begin_date) as cal_mth_num, 
cal_year_mon_week_begin_date, 
cal_year_mon_week_end_date, 
row_number() over(partition by extract('year' from cal_year_mon_week_begin_date) order by cal_year_mon_week_begin_date) as cal_year_week_num_mon 
from {{source('cdm','dim_date')}} 
where cal_year_mon_week_begin_date >= trunc(dateadd('year',-2,date_trunc('year',getdate()))) 
and cal_year_mon_week_end_date < date_trunc('week',getdate()) 
group by 1,2,3,4 
