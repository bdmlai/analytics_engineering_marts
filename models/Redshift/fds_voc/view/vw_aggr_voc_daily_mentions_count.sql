{{
  config({
		 'schema': 'fds_voc',"materialized": 'view',"tags": 'Phase 8',"persist_docs": {'relation' : true, 'columns' : true},
		 "post-hook" : 'grant select on {{this}} to public'
        })
}}
select * from {{ref('aggr_voc_daily_mentions_count')}}