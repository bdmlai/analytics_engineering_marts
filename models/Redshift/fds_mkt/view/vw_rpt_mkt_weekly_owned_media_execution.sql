{{
  config({
		 'schema': 'fds_mkt',	
	     "materialized": 'view'
        })
}}
select * from {{ref('rpt_mkt_weekly_owned_media_execution')}}