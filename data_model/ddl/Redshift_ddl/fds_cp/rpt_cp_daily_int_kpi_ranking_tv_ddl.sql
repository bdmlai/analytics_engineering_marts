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
Enhancement PSTA-1897 Hima Dasan 12/21/2020 Added columns series_Type and telecasr_hours as part of enhancement */

CREATE TABLE
    fds_cp.rpt_cp_daily_int_kpi_ranking_tv
    (
        week DATE ENCODE AZ64,
        DATE TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        country CHARACTER VARYING(150) ENCODE LZO,
        region CHARACTER VARYING(100) ENCODE LZO,
        program CHARACTER VARYING(100) ENCODE LZO,
	series_type CHARACTER VARYING(30) ENCODE LZO,
        telecasts BIGINT ENCODE AZ64,
	telecast_hours NUMERIC(38,2) ENCODE AZ64,
        weekly_aud NUMERIC(38,2) ENCODE AZ64,
        duration_mins BIGINT ENCODE AZ64,
        population BIGINT ENCODE AZ64,
          etl_batch_id CHARACTER VARYING(30) ENCODE LZO,
          etl_insert_user_id CHARACTER VARYING(20) ENCODE LZO,
          etl_insert_rec_dttm TIMESTAMP WITH TIME ZONE ENCODE AZ64,
          etl_update_user_id CHARACTER VARYING(20) ENCODE LZO,
          etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64

    );


