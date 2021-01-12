/*
*******************************************************************************************
Date : 11/13/2020
Version : 1.0
TableName : rpt_cp_daily_followership_by_platform_digitalsnapshot
Schema : fds_cp
Contributor : Remya K Nair
Description : Reporting table consists of monthly followership,views,hrs_watched count for differenct platforms - youtube,dotcom and social media at country level
JIRA : PSTA-1281
***************************************************************************************************
Updates
TYPE JIRA DEVELOPER DATE DESCRIPTION
----- --------- ----- -----------
NEW PSTA-1281  11/13/2020 Initial Version  */

COMMENT ON COLUMN fds_cp.rpt_cp_daily_followership_by_platform_digitalsnapshot.MONTH IS 'Indicates month starting date';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_followership_by_platform_digitalsnapshot.country_name IS 'Represents country name';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_followership_by_platform_digitalsnapshot.content_type IS 'Represents the metric name';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_followership_by_platform_digitalsnapshot.page IS 'Indicates the page name';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_followership_by_platform_digitalsnapshot.followers_count IS 'Indicates value of each metric';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_followership_by_platform_digitalsnapshot.platform IS 'Indicates platforms like youtube,dotcom,social media';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_followership_by_platform_digitalsnapshot.etl_batch_id IS 'Unique ID of DBT Job used to insert the record';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_followership_by_platform_digitalsnapshot.etl_insert_user_id IS 'Unique ID of the DBT user that was used to insert the record';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_followership_by_platform_digitalsnapshot.etl_insert_rec_dttm IS 'Date Time information on when the DBT inserted the record';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_followership_by_platform_digitalsnapshot.etl_update_user_id IS 'Unique ID of the DBT user which was used to update the record manually';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_followership_by_platform_digitalsnapshot.etl_update_rec_dttm IS 'Date Time information on when the record was updated';    	
  	  
