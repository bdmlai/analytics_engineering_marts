/*
*******************************************************************************************
Date : 11/13/2020
Version : 1.0
TableName : rpt_cp_daily_int_kpi_ranking_dotcom
Schema : fds_cp
Contributor : Hima Dasan
Description : Reporting table consists of  daily international KPI ranking data for Dotcom .Has metrics like unique_visitors,visits,page_views,video_views,hours_watched,population.
JIRA : PSTA-1538
***************************************************************************************************
Updates
TYPE JIRA DEVELOPER DATE DESCRIPTION
----- --------- ----- -----------
NEW PSTA-1538 Hima Dasan 11/13/2020 Initial Version  */

COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_dotcom.date IS 'indicates the starting day of each month';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_dotcom.region IS 'represents the region name';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_dotcom.country IS 'represents the country name';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_dotcom.property IS 'Indicates property like WWE.com';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_dotcom.unique_visitors IS 'Number of unique visitors per month';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_dotcom.visits IS 'indicates number of visits';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_dotcom.page_views IS 'Indicates the page views';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_dotcom.video_views IS 'Indicates the video views';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_dotcom.hours_watched IS 'indicates the hours watched';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_dotcom.etl_batch_id IS 'Unique ID of DBT Job used to insert the record';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_dotcom.etl_insert_user_id IS 'Unique ID of the DBT user that was used to insert the record';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_dotcom.etl_insert_rec_dttm IS 'Date Time information on when the DBT inserted the record';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_dotcom.etl_update_user_id IS 'Unique ID of the DBT user which was used to update the record manually';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_dotcom.etl_update_rec_dttm IS 'Date Time information on when the record was updated';    	
  	  

  
