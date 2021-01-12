insert into fds_nplus.rpt_nplus_daily_milestone_complete_rates
select 
          type ,
          external_id ,
          series_episode as title ,
	  date as premiere_date ,
	  complete_rate ,
          viewers  ,
          'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_content' as etl_batch_id,
          'bi_dbt_user_prd' as etl_insert_user_id, 
          current_timestamp as etl_insert_rec_dttm, 
          null as etl_update_user_id, 
          cast(null as timestamp) as etl_update_rec_dttm
from hive_udl_cp.da_static_milestone_complete_rates ;