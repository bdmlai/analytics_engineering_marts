
{{
  config(
	materialized='ephemeral'
    
  )
}}

select * from fds_tw.fact_tw_consumption_post

