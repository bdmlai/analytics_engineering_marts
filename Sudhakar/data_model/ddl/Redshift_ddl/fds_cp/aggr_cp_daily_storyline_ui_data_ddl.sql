/*
*******************************************************************************************
Date : 01/29/2021
Version : 1.0
TableName : aggr_cp_daily_storyline_ui_data
Schema : fds_cp
Contributor : Hima Dasan
Description : Aggregation based on story,show and date level.This data used to issue weekly storyline report after shows and support adhoc/forcasting analysis on storyline level .
JIRA : PSTA-2158
***************************************************************************************************
Updates
TYPE JIRA DEVELOPER DATE DESCRIPTION
----- --------- ----- -----------
NEW PSTA-1897 Hima Dasan 01/29/2021 Initial Version */

CREATE TABLE
    aggr_cp_daily_storyline_ui_data
    (
        show_date DATE ENCODE AZ64,
        show_name CHARACTER VARYING(50) ENCODE LZO,
        story CHARACTER VARYING(500) ENCODE LZO,
        talent CHARACTER VARYING(65535) ENCODE LZO,
        comments CHARACTER VARYING(65535) ENCODE LZO,
        appeared CHARACTER VARYING(65535) ENCODE LZO,
        average_viewers_p18_49 NUMERIC(30,9) ENCODE AZ64,
        avg_p2_plus_viewership NUMERIC(30,9) ENCODE AZ64,
        agg_nielsen_tw_interactions BIGINT ENCODE AZ64,
        tw_interactions_ranking BIGINT ENCODE AZ64,
        etl_batch_id CHARACTER VARYING(26) ENCODE LZO,
        etl_insert_user_id CHARACTER VARYING(15) ENCODE LZO,
        etl_insert_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        etl_update_user_id CHARACTER VARYING(1) ENCODE LZO,
        etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64
    );

