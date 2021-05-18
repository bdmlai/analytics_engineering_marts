{{
  config({
		"materialized": 'ephemeral'
  })
}}
select right(post_match_topic,10) as show_date
        ,event_nm as show_name
        ,post_match_topic_details as segment_name        
        ,theme
        ,link  
        ,reaction_speculation 
 from {{source('hive_ml_clu_sum','process_data')}} a
 left join {{source('cdm','dim_event')}} b on right(post_match_topic,10) = trunc(event_dttm)      
 where   cancellation_date is null 
   and   event_nm is not null
   and   event_nm not in ('UKC', 'CMB', '')
   and   split_part(post_match_topic,' ',1) in ('raw','smackdown','Nxt')
   and   split_part(post_match_topic, ' ', 4) like '20%'
   and    segment_name is not null 
   and    segment_name <> ''
