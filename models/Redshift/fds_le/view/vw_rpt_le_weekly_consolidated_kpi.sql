{{
  config({
	"schema": 'fds_le',
    "materialized": "view"
  })
}}
select * from {{ref('rpt_le_weekly_consolidated_kpi')}}