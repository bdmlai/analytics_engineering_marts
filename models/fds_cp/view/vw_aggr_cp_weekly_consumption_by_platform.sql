{{
  config({
	"schema": 'dwh_read_write',
    "materialized": "view"
  })
}}

select * from {{ref('aggr_cp_weekly_consumption_by_platform')}}