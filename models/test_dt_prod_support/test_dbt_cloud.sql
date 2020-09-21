 {{
  config({
	"schema": 'dt_prod_support',	
	"materialized": 'table'
	})
}}

select * from dt_prod_support.agg_yt_monetization_summary limit 100

