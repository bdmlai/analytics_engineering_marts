{{
  config({
	"schema": 'fds_cp',
    "materialized": "view"
  })
}}

select * from {{ref('agg_cp_weekly_consumption_by_platform')}}