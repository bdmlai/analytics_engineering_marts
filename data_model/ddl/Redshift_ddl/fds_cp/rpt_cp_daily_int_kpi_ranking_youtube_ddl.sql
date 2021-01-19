/*
*******************************************************************************************
Date : 11/13/2020
Version : 1.0
TableName : rpt_cp_daily_int_kpi_ranking_youtube
Schema : fds_cp
Contributor : Hima Dasan
Description : Reporting table consists of  daily international KPI ranking data for Youtube .Has metrics like views,netsubs,hours_watched,revenue,gross_revenue,population.
JIRA : PSTA-1538
***************************************************************************************************
Updates
TYPE JIRA DEVELOPER DATE DESCRIPTION
----- --------- ----- -----------
NEW PSTA-1538 Hima Dasan 11/13/2020 Initial Version  */

CREATE TABLE
    fds_cp.rpt_cp_daily_int_kpi_ranking_youtube 
    (
        country_code CHARACTER VARYING(30) ENCODE LZO,
        year_month CHARACTER VARYING(20) ENCODE LZO,
        channel_name CHARACTER VARYING(5) ENCODE LZO,
        content_type CHARACTER VARYING(30) ENCODE LZO,
        recency CHARACTER VARYING(20) ENCODE LZO,
        views BIGINT ENCODE AZ64,
        netsubs BIGINT ENCODE AZ64,
        hours_watched DOUBLE PRECISION,
        revenue DOUBLE PRECISION,
        gross_revenue DOUBLE PRECISION,
        country_nm CHARACTER VARYING(100) ENCODE LZO,
        region CHARACTER VARYING(100) ENCODE LZO,
        population BIGINT ENCODE AZ64,
          etl_batch_id CHARACTER VARYING(30) ENCODE LZO,
          etl_insert_user_id CHARACTER VARYING(20) ENCODE LZO,
          etl_insert_rec_dttm TIMESTAMP WITH TIME ZONE ENCODE AZ64,
          etl_update_user_id CHARACTER VARYING(20) ENCODE LZO,
          etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64

    );


