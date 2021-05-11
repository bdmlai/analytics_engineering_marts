{{
  config({
	"schema": 'fds_le',
    "materialized": "view","post-hook" : 'grant select on {{this}} to public'
  })
}}
select * from {{ref('rpt_le_weekly_consolidated_kpi')}}