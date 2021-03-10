{{
  config({
		'schema': 'fds_cp',
		"materialized": 'view'
  })
}}
select * from {{ref('rpt_cp_monthly_talent_ranking')}}