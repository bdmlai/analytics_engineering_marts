{{
  config({
	"schema": 'fds_cp',
    "materialized": "view"
  })
}}

select * from {{ref('aggr_cp_weekly_consumption_by_platform')}}