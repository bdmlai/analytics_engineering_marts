{{
  config({
		'schema': 'fds_cp',
		"materialized": 'view',
		"post-hook" : 'grant select on {{this}} to public'
  })
}}
select *
from {{ref('rpt_cp_daily_followership_by_platform_digitalsnapshot')}}