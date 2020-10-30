insert into fds_nplus.rpt_nplus_daily_live_streams
select 
          date as airdate,
	  external_id ,
	  series_episode as title ,
	  segementtype as segmenttype ,
	  content_duration,
	  /*((substring(content_duration, 1, 2))::int * 3600)
	  +((substring(content_duration, 4, 2))::int * 60)
	  +((substring(content_duration, 7, 2))::int) as content_duration ,*/
	  seg_num ,
	  milestone ,
	  matchtype ,
	  talentactions ,
	  move ,
         finishtype ,
	  additionaltalent ,
	  venuelocation ,
          venuename ,
	  nvl(upper(right(trim(venuelocation),2)),null) as state,
	  null as begindate ,
	  null as enddate ,
	  null as nxt_seg_begindate ,
	  null as intvl_dttm ,
	  timestamp as time_interval ,
	  null as prev_time_interval ,
	  streams ,
	  cum_unique_users ,
	  added ,
	  exits ,
	  users ,
	  null as previous_users ,
	  'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_content' as etl_batch_id,
          'bi_dbt_user_prd' as etl_insert_user_id, 
          current_timestamp as etl_insert_rec_dttm, 
          null as etl_update_user_id, 
          cast(null as timestamp) as etl_update_rec_dttm
from hive_udl_cp.da_static_205live_stream_detail ;
--where date >='2016-01-01';