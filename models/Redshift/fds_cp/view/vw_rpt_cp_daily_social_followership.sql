{{
  config({
		 'schema': 'fds_cp',	
	     "materialized": 'view',"tags": 'Content Analytics',"persist_docs": {'relation' : true, 'columns' : true},
		 "post-hook" : 'grant select on {{this}} to public'
        })
}}

select * from {{ref('rpt_cp_daily_social_followership')}}