{{
  config({
		 'schema': 'fds_cpg',	
	     "materialized": 'view',"tags": 'aggr_cpg_daily_sales',"persist_docs": {'relation' : true, 'columns' : true},
	      "post-hook" : 'grant select on {{this}} to public'
        })
}}
select * from {{ref('aggr_cpg_daily_sales')}}