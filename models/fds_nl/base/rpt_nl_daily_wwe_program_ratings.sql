{{
  config(
	materialized='ephemeral'
  )
}}
select * from fds_nl.rpt_nl_daily_wwe_program_ratings