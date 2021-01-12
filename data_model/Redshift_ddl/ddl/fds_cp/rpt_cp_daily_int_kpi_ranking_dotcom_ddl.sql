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

CREATE TABLE
    fds_cp.rpt_cp_daily_int_kpi_ranking_dotcom 
    (
       date DATE ENCODE LZO,
	   region CHARACTER VARYING(100) ENCODE ZSTD,
	   country CHARACTER VARYING(100) ENCODE ZSTD,
	   property CHARACTER VARYING(100) ENCODE ZSTD,
	   unique_visitors BIGINT ENCODE LZO,
	   visits BIGINT ENCODE LZO,
	   page_views BIGINT ENCODE LZO,
	   video_views BIGINT ENCODE LZO,
	   hours_watched INTEGER ENCODE ZSTD,
	   population BIGINT ENCODE ZSTD,
	   etl_batch_id CHARACTER VARYING(30) ENCODE LZO,
          etl_insert_user_id CHARACTER VARYING(20) ENCODE LZO,
          etl_insert_rec_dttm TIMESTAMP WITH TIME ZONE ENCODE AZ64,
          etl_update_user_id CHARACTER VARYING(20) ENCODE LZO,
          etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64

    );
