{{
  config({
		 'schema': 'fds_cpg',"materialized": 'view',"tags": 'Phase 5B',
		 "persist_docs": {'relation' : true, 'columns' : true},
		 "post-hook": 'grant select on {{this}} to public'
        })
}}
select * from {{ref('rpt_cpg_monthly_talent_top25_shop_sales')}}