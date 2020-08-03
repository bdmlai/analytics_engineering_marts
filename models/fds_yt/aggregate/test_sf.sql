{{
  config({
	"schema": 'fds_cp',
    "materialized": "incremental"
  })
}}
select * from cdm.dim_region_country