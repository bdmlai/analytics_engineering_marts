/*
*******************************************************************************************
Date : 12/21/2020
Version : 1.0
TableName : rpt_yt_daily_consumption_bychannel
Schema : fds_yt
Contributor : Hima Dasan
Description : Reporting table consists of daily youtube consumption by channel details . Has metrics like views,likes,dislikes,watched minutes,subscribers gained and lost,revenue views etc for respective country ,channel,content type on daily basis.
JIRA : PSTA-1954
***************************************************************************************************
Updates
TYPE JIRA DEVELOPER DATE DESCRIPTION
----- --------- ----- -----------
NEW PSTA-1954 Hima Dasan 12/21/2020 Initial Version */

CREATE TABLE
    fds_yt.rpt_yt_daily_consumption_bychannel 
    (
       date DATE ENCODE LZO,
        report_date INTEGER ENCODE LZO,
        channel_name CHARACTER VARYING(256) ENCODE LZO,
        country_code CHARACTER VARYING(10) ENCODE LZO,
       content_type CHARACTER VARYING(5) ENCODE LZO,
		country_name CHARACTER VARYING(256) ENCODE LZO,
		region_name CHARACTER VARYING(256) ENCODE LZO,
		views BIGINT ENCODE LZO ,
        likes BIGINT ENCODE LZO,
        dislikes BIGINT ENCODE LZO,
		watch_time_mins DOUBLE PRECISION,
        subscribers_gained BIGINT ENCODE LZO,
        subscribers_lost BIGINT ENCODE LZO,
		 revenue_views BIGINT ENCODE LZO,
        ad_impressions BIGINT ENCODE LZO,
		estimated_partner_revenue DOUBLE PRECISION,
		estimated_partner_ad_revenue DOUBLE PRECISION,
		male BIGINT ENCODE LZO,
        female BIGINT ENCODE LZO,
        gender_other BIGINT ENCODE LZO,
        age_13_17 BIGINT ENCODE LZO,
        age_18_24 BIGINT ENCODE LZO,
        age_25_34 BIGINT ENCODE LZO,
        age_35_44 BIGINT ENCODE LZO,
        age_45_54 BIGINT ENCODE LZO,
        age_55_64 BIGINT ENCODE LZO,
        age_65_and_above BIGINT ENCODE LZO,
          etl_batch_id CHARACTER VARYING(30) ENCODE LZO,
          etl_insert_user_id CHARACTER VARYING(20) ENCODE LZO,
          etl_insert_rec_dttm TIMESTAMP WITH TIME ZONE ENCODE AZ64,
          etl_update_user_id CHARACTER VARYING(20) ENCODE LZO,
          etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64

    );


