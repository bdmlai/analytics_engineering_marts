/*
**********************************************************************************************
Date : 07/09/2020
Version : 2.0
ViewName : aggr_cp_weekly_consumption_by_platform
Schema : fds_cp
Contributor : Sandeep Battula
Frequency : Weekly Wednesday 10:30 AM
Description : aggr_cp_weekly_consumption_by_platform This aggregate table stores the crossplatform consumption metrics - total views and total minutes watched aggregated for each week for platforms- Youtube, facebook, Twitter, Instagram, Snapchat and dotcom/App.
*********************************************************************************************************************************/

CREATE TABLE
    fds_cp.aggr_cp_weekly_consumption_by_platform
    (
        platform CHARACTER VARYING(255) ENCODE LZO,
        monday_date TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        views BIGINT ENCODE AZ64,
        minutes_watched DOUBLE PRECISION,
        prev_views BIGINT ENCODE AZ64,
        prev_mins DOUBLE PRECISION,
        weekly_per_change_views DOUBLE PRECISION,
        weekly_per_change_mins DOUBLE PRECISION,
        etl_batch_id CHARACTER VARYING(100) ENCODE LZO,
        etl_insert_user_id CHARACTER VARYING(255) ENCODE LZO,
        etl_insert_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        etl_update_user_id CHARACTER VARYING(255) ENCODE LZO,
        etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64
    );
	
	
