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

CREATE TABLE
    fds_cp.rpt_cp_daily_int_kpi_ranking_network 
    (
       MONTH TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        country_nm CHARACTER VARYING(100) ENCODE LZO,
        region_nm CHARACTER VARYING(100) ENCODE LZO,
        active_subs BIGINT ENCODE AZ64,
        hours_watched NUMERIC(38,10) ENCODE AZ64,
	views BIGINT ENCODE LZO,
        population BIGINT ENCODE AZ64,
          etl_batch_id CHARACTER VARYING(30) ENCODE LZO,
          etl_insert_user_id CHARACTER VARYING(20) ENCODE LZO,
          etl_insert_rec_dttm TIMESTAMP WITH TIME ZONE ENCODE AZ64,
          etl_update_user_id CHARACTER VARYING(20) ENCODE LZO,
          etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64

    );


