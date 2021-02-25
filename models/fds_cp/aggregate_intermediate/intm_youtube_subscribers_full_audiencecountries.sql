{{
    config({
        "materialized": 'table',
		'schema': 'fds_cp'
         })
			
}}


select * from {{source('hive_udl_yt','youtube_subscribers_full_audiencecountries')}}   
