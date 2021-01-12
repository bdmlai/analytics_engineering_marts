{{
  config({
		 'schema': 'fds_cp',	
	     "materialized": 'view'
        })
}}
select * from {{ref('rpt_cp_monthly_social_overview')}}