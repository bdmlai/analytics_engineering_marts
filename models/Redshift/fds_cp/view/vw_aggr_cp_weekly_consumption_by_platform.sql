{{
  config({
	"schema": 'fds_cp',
    "materialized": "view",
	"post-hook" : 'grant select on {{this}} to public'
  })
}}

select * from {{ref('aggr_cp_weekly_consumption_by_platform')}}