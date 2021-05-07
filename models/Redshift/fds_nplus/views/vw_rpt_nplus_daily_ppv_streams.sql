{{
  config({
	'schema': 'fds_nplus',"materialized": 'view','tags': "rpt_nplus_daily_ppv_streams","persist_docs": {'relation' : true, 'columns' : true},
	'post-hook': 'grant select on {{ this }} to public'
	})
}}
select    premiere_date ,
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
	  users_exits ,
	  total_user ,
	  previous_seg_users 
	 
from {{ref('rpt_nplus_daily_ppv_streams')}}