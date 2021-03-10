{{
  config({
	'schema': 'fds_nplus',
    "materialized": 'view'
	})
}}
select * from {{ref('drvd_intra_hour_quarter_hour_adds')}}