{{
  config({
		 'schema': 'fds_mkt',	
	     "materialized": 'view',"post-hook" : 'grant select on {{this}} to public'
        })
}}
select * from {{ref('rpt_mkt_weekly_owned_media_execution')}}