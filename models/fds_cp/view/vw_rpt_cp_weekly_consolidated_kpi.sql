{{
  config({
	"schema": 'fds_cp',
    "materialized": "view"
  })
}}
select * from {{ref('rpt_cp_weekly_consolidated_kpi')}}

