{{
  config(
	materialized='ephemeral'
    
  )
}}

select * from fds_nplus.AGGR_Daily_SUBSCRIPTION
     