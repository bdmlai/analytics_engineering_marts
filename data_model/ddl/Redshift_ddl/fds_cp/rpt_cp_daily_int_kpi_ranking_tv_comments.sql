
/*
*******************************************************************************************
Date : 11/13/2020
Version : 1.0
TableName : rpt_cp_daily_int_kpi_ranking_tv
Schema : fds_cp
Contributor : Hima Dasan
Description : Reporting table consists of  daily international KPI ranking data for TV .Has metrics like telecasts,weekly_aud,duration_mins,population.
JIRA : PSTA-1538
***************************************************************************************************
Updates
TYPE JIRA DEVELOPER DATE DESCRIPTION
----- --------- ----- -----------
NEW PSTA-1538 Hima Dasan 11/13/2020 Initial Version
Enhancement PSTA-1897 Hima Dasan 12/21/2020 Added columns series_Type and telecast_hours as part of enhancement  */

COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_tv.week IS 'Calendar Year Week Start Date based on Broadcast Date';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_tv.date IS 'indicates the starting day of each month';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_tv.region IS 'The region of country from where the WWE program is telecasted';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_tv.country IS 'The country from where the WWE program is telecasted';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_tv.program IS 'Indicates program name .The value will be Others for the programs other than RAW, SMACKDOWN, NXT, PPV, SUNDAY DHAMAAL, SATURDAY NIGHT, TOTAL BELLAS and TOTAL DIVAS';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_tv.series_type IS 'Indicates WWE series type';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_tv.telecasts IS 'indicates total count of telecasts';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_tv.telecast_hours IS 'indicates total telecast hours';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_tv.weekly_aud IS 'The weekly cumulative audience';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_tv.duration_mins IS 'The duration of WWE Program in minutes';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_tv.population IS 'indicates population value';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_tv.etl_batch_id IS 'Unique ID of DBT Job used to insert the record';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_tv.etl_insert_user_id IS 'Unique ID of the DBT user that was used to insert the record';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_tv.etl_insert_rec_dttm IS 'Date Time information on when the DBT inserted the record';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_tv.etl_update_user_id IS 'Unique ID of the DBT user which was used to update the record manually';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_tv.etl_update_rec_dttm IS 'Date Time information on when the record was updated';    	







