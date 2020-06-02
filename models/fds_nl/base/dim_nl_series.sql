{{
  config(
	materialized='ephemeral'
  )
}}
select * from fds_nl.dim_nl_series