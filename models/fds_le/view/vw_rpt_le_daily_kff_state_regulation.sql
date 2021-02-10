{{
  config({
		 'schema': 'fds_le',"materialized": 'view',"tags": 'Phase 5A',"persist_docs": {'relation' : true, 'columns' : true}
        })
}}
select * from {{ref('rpt_le_daily_kff_state_regulation')}}