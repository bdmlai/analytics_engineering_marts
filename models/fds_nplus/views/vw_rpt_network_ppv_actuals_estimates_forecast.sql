 {{
  config({
	"schemas": 'fds_nplus',	
	"materialized": 'view'
		})
}}


select * from {{ref('rpt_network_ppv_actuals_estimates_forecast')}}