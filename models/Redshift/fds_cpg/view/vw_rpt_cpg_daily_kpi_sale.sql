{{
  config({
		 'schema': 'fds_cpg',"materialized": 'view',"tags": 'Phase 5B',"persist_docs": {'relation' : true, 'columns' : true},
		 "post-hook" : 'grant select on {{this}} to public'
        })
}}
select * from {{ref('rpt_cpg_daily_kpi_sale')}}