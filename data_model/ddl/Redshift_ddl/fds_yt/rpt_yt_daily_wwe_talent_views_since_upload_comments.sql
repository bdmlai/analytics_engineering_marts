/*
************************************************************************************ 
    Date: 01/15/2021
    Version: 1.0
    Description: First Draft
    Contributor: Remya K Nair
    Adl' notes:  
    updated comments
**************************************************************************************/
/* date      | by      |    Details   */
/* 01/15/2021 | Remya K Nair | Report table consists of total views for individual talends at video_id level- PSTA-1824 */


COMMENT ON TABLE fds_yt.rpt_yt_daily_wwe_talent_views_since_upload
IS
    'Report table consists of total views,views for 30 days from uploaded date,unique index,popularity index,video count for individual talents against country since upload';
     COMMENT ON COLUMN fds_yt.rpt_yt_daily_wwe_talent_views_since_upload.region_code
IS
     'This dimension indicates whether the user activity metrics in the data row are associated with viewers who were subscribed to the videos or playlists channel. Possible values are subscribed and unsubscribed.';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_wwe_talent_views_since_upload.talent
IS
     'List of talents';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_wwe_talent_views_since_upload.granularity
IS
     'Since upload';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_wwe_talent_views_since_upload.total_views
IS
     'Total number of times that a video was viewed. In a playlist report, the metric indicates the number of times that a video was viewed in the context of a playlist.';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_wwe_talent_views_since_upload.views_30days
IS
     'Total views with in 30 days from uploaded date';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_wwe_talent_views_since_upload.cnt_video_id
IS
     'Count of videos for each talents against region  since upload';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_wwe_talent_views_since_upload.etl_batch_id
IS
    'Unique ID of DBT Job used to insert the record';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_wwe_talent_views_since_upload.etl_insert_user_id
IS
    'Unique ID of the DBT user that was used to insert the record';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_wwe_talent_views_since_upload.etl_insert_rec_dttm
IS
    'Date Time information on when the DBT inserted the record';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_wwe_talent_views_since_upload.etl_update_user_id
IS
    'Unique ID of the DBT user which was used to update the record manually';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_wwe_talent_views_since_upload.etl_update_rec_dttm
IS
    'Date Time information on when the record was updated';