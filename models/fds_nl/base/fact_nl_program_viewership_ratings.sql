{{
  config(
	materialized='ephemeral'
  )
}}
select * from fds_nl.fact_nl_program_viewership_ratings