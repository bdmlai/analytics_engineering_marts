
{{
  config({
		 'schema': 'fds_cpg',"materialized": 'view',"tags": 'rpt_cpg_weekly_consolidated_kpi',"persist_docs": {'relation' : true, 'columns' : true},
	"post-hook" : 'grant select on fds_cpg.vw_rpt_cpg_weekly_consolidated_kpi to public'

        })
}}

select granularity,
platform,
type,
metric,
year,
month,
week,
start_date,
end_date,
value,
prev_year,
prev_year_week,
prev_year_start_date,
prev_year_end_date,
prev_year_value
 from {{ref('rpt_cpg_weekly_consolidated_kpi')}}
