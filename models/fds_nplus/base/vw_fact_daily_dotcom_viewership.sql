{{
  config(
	materialized='ephemeral'
    
  )
}}

select * from fds_nplus.vw_fact_daily_dotcom_viewership