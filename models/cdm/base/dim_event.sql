{{
  config(
	materialized='ephemeral'
    
  )
}}

select * from cdm.dim_event
