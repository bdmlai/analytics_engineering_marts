/*
*******************************************************************************************
Date : 10/27/2020
Version : 1.0
TableName : rpt_nplus_daily_ppv_streams
Schema : fds_nplus
Contributor : Remya K Nair
Description : Reporting table consists of  viewership by segment details  for PPV Streams
JIRA : PSTA-1045
***************************************************************************************************
Updates
TYPE JIRA DEVELOPER DATE DESCRIPTION
----- --------- ----- -----------
NEW PSTA-1045 Remya K Nair 10/27/2020 Initial Version  */


COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.premiere_date IS 'Date of the content when it debuted';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.external_id IS 'Network ID of the content played on the stream';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.title IS 'Title of the content when it debuted';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.segmenttype IS 'Type of Segment ';	
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.content_duration IS 'Duration of the content';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.seg_num IS 'Segement Number';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.milestone IS 'The comment about the segment';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.matchtype IS 'Type of match';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.talentactions IS 'Describing the actions of talents';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.move IS 'Describing the move';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.finishtype IS 'The type of finish';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.additionaltalent IS 'Additional talent';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.venuelocation IS 'Venue Location of the Program';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.venuename IS 'Venue Name of the Program';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.state IS 'Short name for venuelocation';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.begindate IS 'Segement begin date time';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.enddate IS 'Segment end date time';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.nxt_seg_begindate IS 'Next segment begin datetime';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.intvl_dttm IS 'Time splitted into 5 minutes interval between premiere date and current date';	
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.time_interval IS 'Derived time intervals by considering in_time_est,out_time_est and intvl_dttm';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.prev_time_interval IS 'Previous time interval';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.streams_count IS 'Count of stream ids';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.cumulative_unique_user IS 'count of cumulative users';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.etl_batch_id IS 'Unique ID of DBT Job used to insert the record';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.etl_insert_user_id IS 'Unique ID of the DBT user that was used to insert the record';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.etl_insert_rec_dttm IS 'Date Time information on when the DBT inserted the record';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.etl_update_user_id IS 'Unique ID of the DBT user which was used to update the record manually';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_ppv_streams.etl_update_rec_dttm IS 'Date Time information on when the record was updated'; 