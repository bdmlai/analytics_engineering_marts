{{
  config(
	materialized='ephemeral'
  )
}}
select * from fds_nl.rpt_nl_daily_wwe_live_commercial_ratings