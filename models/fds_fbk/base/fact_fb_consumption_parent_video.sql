
{{
  config(
	materialized='ephemeral'
    
  )
}}

select * from fds_fbk.fact_fb_consumption_parent_video