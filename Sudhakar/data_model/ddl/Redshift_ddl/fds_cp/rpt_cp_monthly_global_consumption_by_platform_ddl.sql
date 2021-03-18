/*
**********************************************************************************************
Date : 07/09/2020
Version : 2.0
ViewName : rpt_cp_monthly_global_consumption_by_platform
Schema : fds_cp
Contributor : Sandeep Battula
Frequency : Weekly Wednesday 10:30 AM
Description : rpt_cp_monthly_global_consumption_by_platform This aggregate table stores the crossplatform consumption metrics - total views and total minutes watched aggregated for each week for platforms- Youtube, facebook, Twitter, Instagram, Snapchat and dotcom/App.
*********************************************************************************************************************************
FIX PSTA-1407    Enhancement is done for Domestic TV platform to get viewing hours for Live+Same day for the dates Live+7 data is not available.  Also updated prehook to delete current month's data for platforms - Domestic TV, PPTV, Hulu SVOD and WWE Network inaddition to current International TV                     																								*/
CREATE TABLE
    fds_cp.rpt_cp_monthly_global_consumption_by_platform
    (
        platform CHARACTER VARYING(16383) ENCODE LZO,
        type CHARACTER VARYING(16383) ENCODE LZO,
        type2 CHARACTER VARYING(16383) ENCODE LZO,
        region CHARACTER VARYING(16383) ENCODE LZO,
        country CHARACTER VARYING(16383) ENCODE LZO,
        MONTH DATE ENCODE AZ64,
        views DOUBLE PRECISION,
        hours_watched DOUBLE PRECISION,
        prev_month_views DOUBLE PRECISION,
        prev_month_hours DOUBLE PRECISION,
        ytd_views DOUBLE PRECISION,
        ytd_hours_watched DOUBLE PRECISION,
        prev_year_views DOUBLE PRECISION,
        prev_year_hours DOUBLE PRECISION,
        etl_batch_id INTEGER ENCODE AZ64,
        etl_insert_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        etl_update_user_id CHARACTER VARYING(1) ENCODE LZO,
        etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        etl_insert_user_id CHARACTER VARYING(15) ENCODE LZO
    );
	
	
