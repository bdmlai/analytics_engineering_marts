/*
************************************************************************************ 
Date : 07/28/2020
Version : 1.0
ViewName : rpt_network_ppv_liveplusvod
Schema : fds_nplus
Contributor : Lakshman Murugeshan
Frequency : Executed for PPV events
Description : View contains the information related to Live NXT and HOF evenet
**************************************************************************************/
/* date      | by      |    Details   */

CREATE TABLE
    fds_nplus.rpt_network_ppv_liveplusvod
    (
        asset_id CHARACTER VARYING(256) ENCODE LZO,
        production_id CHARACTER VARYING(384) ENCODE LZO,
        event CHARACTER VARYING(268) ENCODE LZO,
        event_name CHARACTER VARYING(268) ENCODE LZO,
        event_date DATE ENCODE AZ64,
        start_time TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        end_time TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        platform CHARACTER VARYING(2000) ENCODE LZO,
        views DOUBLE PRECISION,
        us_views BIGINT ENCODE AZ64,
        minutes DOUBLE PRECISION,
        per_us_views DOUBLE PRECISION,
        prev_month_views DOUBLE PRECISION,
        prev_month_event CHARACTER VARYING(268) ENCODE LZO,
        prev_year_views DOUBLE PRECISION,
        prev_year_event CHARACTER VARYING(268) ENCODE LZO,
        monthly_per_change_views DOUBLE PRECISION,
        yearly_per_change_views DOUBLE PRECISION,
        duration DOUBLE PRECISION,
        overall_rank BIGINT ENCODE AZ64,
        yearly_rank BIGINT ENCODE AZ64,
        tier CHARACTER VARYING(6) ENCODE LZO,
        monthly_color CHARACTER VARYING(1) ENCODE LZO,
        yearly_color CHARACTER VARYING(1) ENCODE LZO,
        choose_ppv CHARACTER VARYING(15) ENCODE LZO,
        event_brand CHARACTER VARYING(250) ENCODE LZO,
        report_name CHARACTER VARYING(100) ENCODE LZO,
        series_name CHARACTER VARYING(100) ENCODE LZO,
        account CHARACTER VARYING(256) ENCODE LZO,
        url CHARACTER VARYING(2000) ENCODE LZO,
        content_wweid CHARACTER VARYING(500) ENCODE LZO,
        data_level CHARACTER VARYING(100) ENCODE LZO,
        etl_batch_id CHARACTER VARYING(27) ENCODE LZO
    );
COMMENT ON TABLE fds_nplus.rpt_network_ppv_liveplusvod
IS
    '## Implementation Detail
*   Date        : 07/28/2020
*   Version     : 1.0
*   ViewName    : fds_nplus.rpt_network_ppv_liveplusvod
*   Schema : fds_nplus
*   Contributor : Lakshman Murugeshan
*   Description : View contains the information related to Live NXT and HOF evenet

## Maintenance Log
* Date : 07/28/2020 ; Developer: Lakshman Murugeshan ; DBT & Python Automation: Sudhakar; Change: Initial Version'
    ;
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.asset_id
IS
    'The ID for a YouTube channel. In the YouTube Data API, this is the value of a channelresources id property.'
    ;
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.production_id
IS
    'Unique content version level id';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.event
IS
    'Event name and year';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.event_name
IS
    'Event name';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.event_date
IS
    'Date of the event';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.start_time
IS
    'start timestamp of the event';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.end_time
IS
    'end timestamp of the event';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.platform
IS
    'platform name i.e. Facebook, Youtube, etc';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.views
IS
    'number of views for the event';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.us_views
IS
    'number of views in US for the event';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.minutes
IS
    'number of minutes watched';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.per_us_views
IS
    'percentage of views in US against overall views';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.prev_month_views
IS
    'previous month views';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.prev_month_event
IS
    'previous month event name';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.prev_year_views
IS
    'previous year views';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.prev_year_event
IS
    'previous year event name';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.monthly_per_change_views
IS
    'monthly percentage changes in the number of views';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.yearly_per_change_views
IS
    'yearly percentage changes in the number of views';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.duration
IS
    'duration of the event';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.overall_rank
IS
    'overall rank of the event based on the views';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.yearly_rank
IS
    'yearly rank of the event based on the views';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.tier
IS
    'event tier';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.monthly_color
IS
    'color metric for the month';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.yearly_color
IS
    'color metric for the year';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.choose_ppv
IS
    'prior or most recent ppv';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.event_brand
IS
    'event brand i.e. PPV, NXT or Hall of Fame';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.report_name
IS
    'name of the report i.e. Kickoff show, The Bump etc';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.series_name
IS
    'name of the series';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.account
IS
    'account name of the platform';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.url
IS
    'platform url of the event';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.content_wweid
IS
    'unique identifier for the event';
COMMENT ON COLUMN fds_nplus.rpt_network_ppv_liveplusvod.data_level
IS
    'identifier for Live data and Live+VOD data';