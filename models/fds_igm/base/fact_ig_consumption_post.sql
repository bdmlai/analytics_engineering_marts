{{
  config(
	materialized='ephemeral'
    
  )
}}

select * from fds_igm.fact_ig_consumption_post


