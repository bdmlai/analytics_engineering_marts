{{
  config({
	"schema": 'fds_cp',
        "materialized": "view",
	"post-hook" : 'grant select on fds_cp.vw_rpt_cp_weekly_consolidated_kpi to public'

  })
}}
SELECT granularity, platform, type, metric, year, month, week, start_date, end_date, value, prev_year, 
prev_year_week, prev_year_start_date, prev_year_end_date, prev_year_value FROM {{ref('rpt_cp_weekly_consolidated_kpi')}}
union
SELECT granularity, platform, type, metric, year, month, week, start_date, end_date, value, prev_year, 
prev_year_week, prev_year_start_date, prev_year_end_date, prev_year_value FROM {{ref('rpt_tv_weekly_consolidated_kpi')}}
union
SELECT granularity, platform, type, metric, year, month, week, start_date, end_date, value, prev_year, 
prev_year_week, prev_year_start_date, prev_year_end_date, prev_year_value FROM {{ref('rpt_le_weekly_consolidated_kpi')}}
union
SELECT granularity, platform, type, metric, year, month, week, start_date, end_date, value, prev_year, 
prev_year_week, prev_year_start_date, prev_year_end_date, prev_year_value FROM fds_cpg.rpt_cpg_weekly_consolidated_kpi
