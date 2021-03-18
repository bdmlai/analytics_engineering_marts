{{
  config({
		"schema": 'fds_nplus',
                "pre-hook": ["delete from fds_nplus.rpt_nplus_daily_ppv_streams where premiere_date >= current_date -7"],
		"materialized": 'incremental','tags': "Content", "persist_docs": {'relation' : true, 'columns' : true}
  })
}}
select *,
(select count(distinct stream_id) from {{ref('intm_nplus_viewership_data_with_externalid')}} b where a.external_id = b.external_id and b.min_time < a.time_interval
) as streams_count,
(select count(distinct src_fan_id) from {{ref('intm_nplus_viewership_data_with_externalid')}} c where a.external_id = c.external_id and (c.min_time < a.time_interval) 
) as cumulative_unique_user,
(select count(distinct src_fan_id) from {{ref('intm_nplus_viewership_data_with_externalid')}}  where a.external_id = external_id and min_time < a.time_interval 
and  min_time > a.prev_time_interval
) as users_added,
(select count(distinct src_fan_id) from {{ref('intm_nplus_viewership_data_with_externalid')}}  where a.external_id = external_id and max_time < a.time_interval and max_time > a.prev_time_interval  
) as users_exits,	
(select count(distinct src_fan_id) from {{ref('intm_nplus_viewership_data_with_externalid')}}  where a.external_id = external_id and min_time < a.time_interval and max_time > a.time_interval  
) as total_user,
(select count(distinct src_fan_id) from {{ref('intm_nplus_viewership_data_with_externalid')}}  where a.external_id = external_id and min_time < a.prev_time_interval and max_time > a.time_interval  
) as previous_seg_users,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_content' as etl_batch_id,
 'bi_dbt_user_prd' as etl_insert_user_id, 
current_timestamp as etl_insert_rec_dttm, 
null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm

from {{ref('intm_nplus_viewership_sequence_generator')}} a