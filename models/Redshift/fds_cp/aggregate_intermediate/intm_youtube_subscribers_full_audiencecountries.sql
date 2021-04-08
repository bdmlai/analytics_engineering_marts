{{
    config({
        "materialized": 'table',"tags": 'cp_monthly_global_followership_by_platform',
	"schema" :'fds_cp'
         })
			
}}


select * from {{source('hive_udl_yt','youtube_subscribers_full_audiencecountries')}}   
