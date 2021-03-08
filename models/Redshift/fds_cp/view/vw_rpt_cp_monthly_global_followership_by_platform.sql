{{
  config({
		 'schema': 'fds_cp',	
	     "materialized": 'view',"tags": 'Content',"persist_docs": {'relation' : true, 'columns' : true}
        })
}}

select * from {{ref('rpt_cp_monthly_global_followership_by_platform')}}


