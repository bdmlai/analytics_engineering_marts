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
    fds_cp.aggr_cp_daily_storyline_ui_data 
    (
        show_date DATE ENCODE AZ64,
        show_name CHARACTER VARYING(50) ENCODE LZO,
        story CHARACTER VARYING(500) ENCODE LZO,
        TALENT CHARACTER VARYING(500) ENCODE LZO,
        COMMENTS CHARACTER VARYING(6000) ENCODE LZO,
        Appeared CHARACTER VARYING(200) ENCODE LZO,
        average_viewers_p18_49 NUMERIC(38,2) ENCODE AZ64,
        avg_p2_plus_viewership NUMERIC(38,2) ENCODE AZ64,
        agg_nielsen_tw_interactions INTEGER ENCODE AZ64,
        tw_interactions_Ranking INTEGER ENCODE AZ64,
        etl_batch_id BIGINT ENCODE AZ64,
        etl_insert_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        etl_insert_user_id CHARACTER VARYING(20) ENCODE LZO,
        etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        etl_update_user_id CHARACTER VARYING(20) ENCODE LZO
	);

