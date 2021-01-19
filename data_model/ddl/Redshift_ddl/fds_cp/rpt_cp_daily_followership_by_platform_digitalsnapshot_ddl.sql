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


create table fds_cp.rpt_cp_daily_followership_by_platform_digitalsnapshot
(
        MONTH CHARACTER VARYING(100) ENCODE LZO,
        country_name CHARACTER VARYING(100) ENCODE LZO,
        content_type CHARACTER VARYING(100) ENCODE LZO,
        page CHARACTER VARYING(200) ENCODE LZO,
        followers_count BIGINT ENCODE AZ64,
        platform CHARACTER VARYING(100) ENCODE LZO,
        etl_batch_id CHARACTER VARYING(100) ENCODE LZO,
        etl_insert_user_id CHARACTER VARYING(50) ENCODE LZO,
        etl_insert_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        etl_update_user_id CHARACTER VARYING(50) ENCODE LZO,
        etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64
)
