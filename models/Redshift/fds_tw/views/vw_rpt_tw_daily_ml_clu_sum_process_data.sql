{{
  config({
	'schema': 'fds_tw',"materialized": 'view','tags': "rpt_tw_daily_ml_clu_sum_process_data" ,"persist_docs": {'relation' : true, 'columns' : true},
          'post-hook': 'grant select on {{ this }} to public'
       
	})
}}


select  a.show_date
        ,a.show_name
        ,a.segment_name        
        ,a.theme       
        ,tweet_count
        ,reaction_count
        ,speculation_count

from {{ref('rpt_tw_daily_ml_clu_sum_process_data')}} a
