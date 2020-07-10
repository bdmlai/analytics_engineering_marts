{{
  config(
	materialized='ephemeral'
  )
}}
select * from fds_nl.fact_nl_timeperiod_viewership_ratings