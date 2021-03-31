{{
  config({
	     'schema': 'fds_cp',	
	     "materialized": 'view','tags': "aggr_cp_daily_storyline_ui_data","persist_docs": {'relation' : true, 'columns' : true} ,
             "post-hook" : 'grant select on {{this}} to public'
        })
}}

select * from {{ref('aggr_cp_daily_storyline_ui_data')}}