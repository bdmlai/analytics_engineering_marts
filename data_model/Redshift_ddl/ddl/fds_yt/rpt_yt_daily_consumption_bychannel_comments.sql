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


COMMENT ON COLUMN fds_yt.rpt_yt_daily_consumption_bychannel.date IS 'Report Date';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_consumption_bychannel.report_date IS 'The date when reports need to generate';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_consumption_bychannel.channel_name IS 'Name of channel defined in yotube metadata';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_consumption_bychannel.country_code IS 'The country associated with the metrics in the report row.';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_consumption_bychannel.content_type IS 'Type 1 or 2 for the video';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_consumption_bychannel.country_name IS 'Indicates country name';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_consumption_bychannel.region_name IS 'Indicates region for the country';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_consumption_bychannel.views IS 'The number of times that a video was viewed. In a playlist report, the metric indicates the number of times that a video was viewed in the context of a playlist.';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_consumption_bychannel.likes IS 'The number of times that users indicated that they liked a video by giving it a positive rating.';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_consumption_bychannel.dislikes IS 'The number of times that users indicated that they disliked a video by giving it a negative rating.';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_consumption_bychannel.watch_time_mins IS 'The number of china Followers';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_consumption_bychannel.subscribers_gained IS 'The number of times that users subscribed to a channel.';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_consumption_bychannel.subscribers_lost IS 'The number of times that users unsubscribed from a channel.';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_consumption_bychannel.revenue_views IS 'Views';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_consumption_bychannel.ad_impressions IS 'The number of verified ad impressions served.';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_consumption_bychannel.estimated_partner_revenue IS 'Estimated Partner Revenue';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_consumption_bychannel.estimated_partner_ad_revenue IS 'Estimated Ad Revenue';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_consumption_bychannel.male IS 'views by male';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_consumption_bychannel.female IS 'female';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_consumption_bychannel.gender_other IS 'views by other gender';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_consumption_bychannel.etl_batch_id IS 'Unique ID of DBT Job used to insert the record';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_consumption_bychannel.etl_insert_user_id IS 'Unique ID of the DBT user that was used to insert the record';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_consumption_bychannel.etl_insert_rec_dttm IS 'Date Time information on when the DBT inserted the record';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_consumption_bychannel.etl_update_user_id IS 'Unique ID of the DBT user which was used to update the record manually';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_consumption_bychannel.etl_update_rec_dttm IS 'Date Time information on when the record was updated';    	







