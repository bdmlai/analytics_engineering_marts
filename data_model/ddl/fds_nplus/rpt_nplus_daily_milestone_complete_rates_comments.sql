/*
*******************************************************************************************
Date : 10/27/2020
Version : 1.0
TableName : rpt_nplus_daily_milestone_complete_rates
Schema : fds_nplus
Contributor : Remya K Nair
Description : Reporting table consists of  Complete rates for PPV,live and tko streams
JIRA : PSTA-1045
***************************************************************************************************
Updates
TYPE JIRA DEVELOPER DATE DESCRIPTION
----- --------- ----- -----------
NEW PSTA-1045 Remya K Nair 10/27/2020 Initial Version  */

COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_milestone_complete_rates.external_id IS 'Network ID of the content played on the stream';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_milestone_complete_rates.title IS 'Title of the content when it debuted';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_milestone_complete_rates.premiere_date IS 'Date of the content when it debuted';
COMMENT ON COLUMN fds_nplus.rpt_nplus_daily_milestone_complete_rates.viewers IS 'count of viewers ';
COMMENT ON COLUMN fds_nplus.rpt_nplus_weekly_cg_ppv_streams.etl_batch_id IS 'Unique ID of DBT Job used to insert the record';
COMMENT ON COLUMN fds_nplus.rpt_nplus_weekly_cg_ppv_streams.etl_insert_user_id IS 'Unique ID of the DBT user that was used to insert the record';
COMMENT ON COLUMN fds_nplus.rpt_nplus_weekly_cg_ppv_streams.etl_insert_rec_dttm IS 'Date Time information on when the DBT inserted the record';
COMMENT ON COLUMN fds_nplus.rpt_nplus_weekly_cg_ppv_streams.etl_update_user_id IS 'Unique ID of the DBT user which was used to update the record manually';
COMMENT ON COLUMN fds_nplus.rpt_nplus_weekly_cg_ppv_streams.etl_update_rec_dttm IS 'Date Time information on when the record was updated';    	
  	  
