{{
  config({
	'schema': 'dt_prod_support',	
	"materialized": 'view',
	"post-hook" : 'grant select on {{this}} to public'
	})
}}

select * from {{ref('rpt_ppv_final_table')}}