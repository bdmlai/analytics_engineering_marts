{{
  config(
	materialized='ephemeral'
    
  )
}}

select * from fds_nplus.aggr_nplus_daily_forcast_output
     