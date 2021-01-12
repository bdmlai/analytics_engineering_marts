/*
************************************************************************************ 
    Date: 12/24/2020
    Version: 1.0
    Description: First Draft
    Contributor: Remya K Nair
    Adl' notes:  
    updated comments
**************************************************************************************/
/* date      | by      |    Details   */
/* 12/24/2020 | Remya K Nair | Report table consists of splitting talents present in talent pool - PSTA-1824 */


COMMENT ON TABLE fds_yt.rpt_yt_daily_wwe_talent
IS
    'Report table consists of splitting talents present in talent pool';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_wwe_talent.report_date
IS
     'Report Date';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_wwe_talent.talent
IS
     'List of talents';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_wwe_talent.video_id
IS
     'The ID of a YouTube video. In the YouTube Data API, this is the value of a video resources idproperty.';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_wwe_talent.region_code
IS
     'This dimension indicates whether the user activity metrics in the data row are associated with viewers who were subscribed to the videos or playlists channel. Possible values are subscribed and unsubscribed.';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_wwe_talent.total_views
IS
     'Total number of times that a video was viewed. In a playlist report, the metric indicates the number of times that a video was viewed in the context of a playlist.';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_wwe_talent.etl_batch_id
IS
    'Unique ID of DBT Job used to insert the record';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_wwe_talent.etl_insert_user_id
IS
    'Unique ID of the DBT user that was used to insert the record';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_wwe_talent.etl_insert_rec_dttm
IS
    'Date Time information on when the DBT inserted the record';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_wwe_talent.etl_update_user_id
IS
    'Unique ID of the DBT user which was used to update the record manually';
COMMENT ON COLUMN fds_yt.rpt_yt_daily_wwe_talent.etl_update_rec_dttm
IS
    'Date Time information on when the record was updated';