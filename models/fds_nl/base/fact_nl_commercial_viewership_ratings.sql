{{
  config(
	materialized='ephemeral'
  )
}}
select * from fds_nl.fact_nl_commercial_viewership_ratings