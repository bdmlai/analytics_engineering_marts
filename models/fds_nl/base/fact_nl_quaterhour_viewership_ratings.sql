{{
  config(
	materialized='ephemeral'
  )
}}
select * from fds_nl.fact_nl_quaterhour_viewership_ratings