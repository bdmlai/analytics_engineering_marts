{{
  config({
	     'schema': 'fds_cp',	
	     "materialized": 'view',"tags": 'Content',"persist_docs": {'relation' : true, 'columns' : true}
        })
}}

select * from {{ref('aggr_cp_daily_storyline_ui_data')}}