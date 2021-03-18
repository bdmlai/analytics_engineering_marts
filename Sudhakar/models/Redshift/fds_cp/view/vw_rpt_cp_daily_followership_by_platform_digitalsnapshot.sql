{{
  config({
		'schema': 'fds_cp',
		"materialized": 'view'
  })
}}
select *
from {{ref('rpt_cp_daily_followership_by_platform_digitalsnapshot')}}