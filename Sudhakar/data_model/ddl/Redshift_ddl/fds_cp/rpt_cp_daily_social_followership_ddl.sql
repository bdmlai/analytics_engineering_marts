/*
*******************************************************************************************
Date : 12/21/2020
Version : 1.0
TableName : rpt_cp_daily_social_followership
Schema : fds_cp
Contributor : Hima Dasan
Description : rpt_cp_daily_social_followership provide Subscribers gains and followers of Facebook, YouTube, Instagram at country level 
JIRA : PSTA-1897
***************************************************************************************************
Updates
TYPE JIRA DEVELOPER DATE DESCRIPTION
----- --------- ----- -----------
NEW PSTA-1897 Hima Dasan 12/21/2020 Initial Version */

CREATE TABLE
    fds_cp.rpt_cp_daily_social_followership 
    (
       MONTH TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        country_nm CHARACTER VARYING(100) ENCODE LZO,
        region_nm CHARACTER VARYING(100) ENCODE LZO,
        fb_gain INTEGER ENCODE ZSTD,
        fb_followers INTEGER ENCODE ZSTD,
		 ig_gain INTEGER ENCODE ZSTD,
        ig_followers INTEGER ENCODE ZSTD,
		 yt_gain INTEGER ENCODE ZSTD,
        yt_followers INTEGER ENCODE ZSTD,
				 china_gain INTEGER ENCODE ZSTD,
        china_followers INTEGER ENCODE ZSTD,
	population BIGINT ENCODE AZ64,
          etl_batch_id CHARACTER VARYING(30) ENCODE LZO,
          etl_insert_user_id CHARACTER VARYING(20) ENCODE LZO,
          etl_insert_rec_dttm TIMESTAMP WITH TIME ZONE ENCODE AZ64,
          etl_update_user_id CHARACTER VARYING(20) ENCODE LZO,
          etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64

    );


