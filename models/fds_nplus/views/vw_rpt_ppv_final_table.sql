{{
  config(
	schema='dt_prod_support',	
	materialized='view'
    
  )
}}
select * from {{ref('rpt_ppv_final_table')}}