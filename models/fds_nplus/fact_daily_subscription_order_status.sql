{{
  config(
	materialized='ephemeral'
    
  )
}}

select * from fds_nplus.fact_daily_subscription_order_status