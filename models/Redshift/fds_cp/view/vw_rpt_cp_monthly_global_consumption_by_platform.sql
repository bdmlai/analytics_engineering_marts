{{
  config({
	"schema": 'fds_cp',
    "materialized": "view"
  })
}}

select * from {{ref('rpt_cp_monthly_global_consumption_by_platform')}}