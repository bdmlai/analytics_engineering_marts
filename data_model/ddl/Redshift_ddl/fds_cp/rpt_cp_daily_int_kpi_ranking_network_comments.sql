/*
*******************************************************************************************
Date : 11/13/2020
Version : 1.0
TableName : rpt_cp_daily_int_kpi_ranking_network
Schema : fds_cp
Contributor : Hima Dasan
Description : Reporting table consists of  daily international KPI ranking data for network .Has metrics like active_subs,hours_watched,population.
JIRA : PSTA-1538
***************************************************************************************************
Updates
TYPE JIRA DEVELOPER DATE DESCRIPTION
----- --------- ----- -----------
NEW PSTA-1538 Hima Dasan 11/13/2020 Initial Version 
Enhancement PSTA-1897 Hima Dasan 12/21/2020 Included metric views as part of enhancement  */

COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_network.month IS 'indicates the calender month';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_network.region IS 'represents the region name';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_network.country IS 'Indicates content type like WWE';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_network.active_subs IS 'Total active subscription';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_network.hours_watched IS 'Monthly hours consumed';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_network.views IS 'Count of views for the month';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_network.population IS 'indicates population values';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_network.etl_batch_id IS 'Unique ID of DBT Job used to insert the record';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_network.etl_insert_user_id IS 'Unique ID of the DBT user that was used to insert the record';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_network.etl_insert_rec_dttm IS 'Date Time information on when the DBT inserted the record';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_network.etl_update_user_id IS 'Unique ID of the DBT user which was used to update the record manually';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_network.etl_update_rec_dttm IS 'Date Time information on when the record was updated';    	
