{{
  config({
		 'schema': 'fds_le',"materialized": 'view',"tags": 'Phase 5A',"persist_docs": {'relation' : true, 'columns' : true}
        })
}}
select * from {{ref('rpt_le_monthly_local_county_ratings')}}