{{
  config(
	materialized='ephemeral'
    
  )
}}

select * from fds_sc.fact_sc_consumption_story