{{
  config({
		 'schema': 'fds_cpg',	
	     "materialized": 'view',"tags": 'Phase 5B',"persist_docs": {'relation' : true, 'columns' : true}
        })
}}
select * from {{ref('aggr_cpg_daily_kit_sales')}}