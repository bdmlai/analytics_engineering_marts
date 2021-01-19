
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

COMMENT ON COLUMN fds_cp.rpt_cp_daily_social_followership.MONTH IS 'Indicates starting day of each month';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_social_followership.country_nm IS 'indicates name of country';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_social_followership.region_nm IS 'Indicates region for the country';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_social_followership.fb_gain IS 'calculated value for face book subscribers gain';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_social_followership.fb_followers IS 'The number of Facebook Followers broken out by Country';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_social_followership.ig_gain IS 'calculated value for insta gram subscribers gain';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_social_followership.ig_followers IS 'The number of Instagram Followers broken out by Country';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_social_followership.yt_gain IS 'calculated value for youtube subscribers gain';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_social_followership.yt_followers IS 'The number of youtube Followers broken out by Country';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_social_followership.china_gain IS 'calculated value for china followers gain';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_social_followership.china_followers IS 'The number of china Followers';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_social_followership.population IS 'countrywise population';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_social_followership.etl_batch_id IS 'Unique ID of DBT Job used to insert the record';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_social_followership.etl_insert_user_id IS 'Unique ID of the DBT user that was used to insert the record';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_social_followership.etl_insert_rec_dttm IS 'Date Time information on when the DBT inserted the record';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_social_followership.etl_update_user_id IS 'Unique ID of the DBT user which was used to update the record manually';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_social_followership.etl_update_rec_dttm IS 'Date Time information on when the record was updated';    	







