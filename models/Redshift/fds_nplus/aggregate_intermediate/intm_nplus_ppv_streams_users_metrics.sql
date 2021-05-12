{{
  config({
		"schema": 'fds_nplus',
		"materialized": 'table','tags': "rpt_nplus_daily_ppv_streams", 
		'post-hook': 'grant select on {{ this }} to public'
  })
}}

select   external_id,
         time_interval,
         prev_time_interval,
         users_added ,
         'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_content' as etl_batch_id,
         'bi_dbt_user_prd' as etl_insert_user_id, 
          current_timestamp as etl_insert_rec_dttm, 
          null as etl_update_user_id,
          cast(null as timestamp) as etl_update_rec_dttm
 from  {{ref('intm_nplus_users_added_ppv')}} 