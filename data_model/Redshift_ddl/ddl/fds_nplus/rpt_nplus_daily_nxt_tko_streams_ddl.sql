/*
*******************************************************************************************
Date : 10/27/2020
Version : 1.0
TableName : rpt_nplus_daily_nxt_tko_streams
Schema : fds_nplus
Contributor : Remya K Nair
Description : Reporting table consists of  viewership by segment details  for tko Streams
JIRA : PSTA-1045
***************************************************************************************************
Updates
TYPE JIRA DEVELOPER DATE DESCRIPTION
----- --------- ----- -----------
NEW PSTA-1045 Remya K Nair 10/27/2020 Initial Version  */


CREATE TABLE
    fds_nplus.rpt_nplus_daily_nxt_tko_streams
    (
	  premiere_date DATE ENCODE LZO,
	  external_id CHARACTER VARYING(256) ENCODE LZO,
	  title CHARACTER VARYING(1000) ENCODE LZO,
	  segmenttype CHARACTER VARYING(200) ENCODE LZO,
	  content_duration DOUBLE PRECISION,
	  seg_num NUMERIC(19,0) ENCODE LZO,
	  milestone CHARACTER VARYING(6000) ENCODE LZO,
	  matchtype CHARACTER VARYING(500),
	  talentactions CHARACTER VARYING(1000) ENCODE LZO,
	  move CHARACTER VARYING(200),
          finishtype CHARACTER VARYING(500),
	  additionaltalent CHARACTER VARYING(65535) ENCODE LZO,
	  venuelocation CHARACTER VARYING(100) ENCODE LZO,
          venuename CHARACTER VARYING(200) ENCODE TEXT255,
	  state CHARACTER VARYING(150) ENCODE LZO,
	  begindate TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
	  enddate TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
	  nxt_seg_begindate TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
	  intvl_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
	  time_interval TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
	  prev_time_interval TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
	  streams_count BIGINT ENCODE LZO,
	  cumulative_unique_user BIGINT ENCODE LZO,
	  users_added BIGINT ENCODE LZO,
	  users_exits BIGINT ENCODE LZO,
	  total_user BIGINT ENCODE LZO,
	  previous_seg_users BIGINT ENCODE LZO,
	  etl_batch_id CHARACTER VARYING(26) ENCODE LZO,
         etl_insert_user_id CHARACTER VARYING(15) ENCODE LZO,
         etl_insert_rec_dttm TIMESTAMP WITH TIME ZONE ENCODE AZ64,
         etl_update_user_id CHARACTER VARYING(1) ENCODE LZO,
         etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64
	  );
