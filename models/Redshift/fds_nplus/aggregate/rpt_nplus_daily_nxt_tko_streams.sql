{{
  config({
		"schema": 'fds_nplus',
		"materialized": 'incremental','tags': "rpt_nplus_daily_nxt_tko_streams", "persist_docs": {'relation' : true, 'columns' : true},
		"post-hook": ['grant select on {{this}} to public', 'drop table dt_prod_support.intm_nplus_nxt_tko_streams_users_metrics']
  })
}}
select premiere_date ,
	  external_id ,
	  title ,
	  segmenttype ,
	  content_duration ,
	  seg_num ,
	  milestone ,
	  matchtype ,
	  talentactions ,
	  move ,
          finishtype,
	  additionaltalent ,
	  venuelocation ,
          venuename ,
	  state ,
	  begindate ,
	  enddate ,
	  nxt_seg_begindate ,
	  intvl_dttm ,
	  time_interval ,
	  prev_time_interval ,
	  streams_count ,
	  cumulative_unique_user ,
	  users_added ,
	  (lag(total_user,1) over (order by time_interval) + users_added - total_user) as users_exits ,
	  total_user ,
	  previous_seg_users ,
	  'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_content' as etl_batch_id,
          'bi_dbt_user_prd' as etl_insert_user_id, 
          current_timestamp as etl_insert_rec_dttm, 
          null as etl_update_user_id, 
          cast(null as timestamp) as etl_update_rec_dttm

 from {{ref('intm_nplus_viewership_final_nxt_tko')}} 

