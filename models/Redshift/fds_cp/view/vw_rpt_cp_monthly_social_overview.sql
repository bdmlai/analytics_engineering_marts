{{
  config({
		 'schema': 'fds_cp',	
	     "materialized": 'view', 
		 'tags': "Content",
		"post-hook": ["grant select on {{this}} to public"]
        })
}}
select * from {{ref('rpt_cp_monthly_social_overview')}}