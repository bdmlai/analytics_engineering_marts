 {{
  config({
	"schemas": 'fds_nplus',	
	"materialized": 'view'
		})
}}


select * from {{ref('rpt_actuals_estimates_forecast_view')}}