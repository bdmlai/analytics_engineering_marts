{{
  config({
		"schema": 'fds_nplus',
                "pre-hook": ["delete from fds_nplus.rpt_nplus_daily_nxt_tko_streams where premiere_date >= current_date -7"],
		"materialized": 'incremental','tags': "Content", "persist_docs": {'relation' : true, 'columns' : true}
  })
}}
select *,
(select count(distinct stream_id) from {{ref('intm_nplus_viewershipdata_with_externalid_nxt_tko')}} b where a.external_id = b.external_id and b.min_time < a.time_interval
) as streams,
(select count(distinct src_fan_id) from {{ref('intm_nplus_viewershipdata_with_externalid_nxt_tko')}} c where a.external_id = c.external_id and (c.min_time < a.time_interval) 
) as cum_unique_users,
(select count(distinct src_fan_id) from {{ref('intm_nplus_viewershipdata_with_externalid_nxt_tko')}}  where a.external_id = external_id and min_time < a.time_interval 
and  min_time > a.prev_time_interval
) as added,
(select count(distinct src_fan_id) from {{ref('intm_nplus_viewershipdata_with_externalid_nxt_tko')}}  where a.external_id = external_id and max_time < a.time_interval and max_time > a.prev_time_interval  
) as exits,	
(select count(distinct src_fan_id) from {{ref('intm_nplus_viewershipdata_with_externalid_nxt_tko')}}  where a.external_id = external_id and min_time < a.time_interval and max_time > a.time_interval  
) as users,
(select count(distinct src_fan_id) from {{ref('intm_nplus_viewershipdata_with_externalid_nxt_tko')}}  where a.external_id = external_id and min_time < a.prev_time_interval and max_time > a.time_interval  
) as previous_users,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_content' as etl_batch_id,
 'bi_dbt_user_prd' as etl_insert_user_id, 
current_timestamp as etl_insert_rec_dttm, 
null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm

from {{ref('intm_nplus_viewership_sequence_generator_nxt_tko')}} a