{{
  config({
		 'schema': 'fds_cpg',"materialized": 'view',"tags": 'Phase 5B',"persist_docs": {'relation' : true, 'columns' : true}
        })
}}
select * from {{ref('rpt_cpg_daily_snapshot_shipped_sale')}}