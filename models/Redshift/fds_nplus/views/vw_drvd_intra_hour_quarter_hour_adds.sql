{{
  config({
	'schema': 'fds_nplus',
    "materialized": 'view',"post-hook" : 'grant select on {{this}} to public'
	})
}}
select * from {{ref('drvd_intra_hour_quarter_hour_adds')}}