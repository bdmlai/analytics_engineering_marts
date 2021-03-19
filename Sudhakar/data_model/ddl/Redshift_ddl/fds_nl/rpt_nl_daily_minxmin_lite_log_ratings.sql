/*
************************************************************************************ 
    Date: 8/25/2020
    Version: 1.0
    Description: First Draft
    Contributor: Rahul Chandran
    Adl' notes:  
    updated comments
**************************************************************************************/
/* date      | by      |    Details   */

CREATE TABLE
    fds_nl.rpt_nl_daily_minxmin_lite_log_ratings
    (
        broadcast_date_id INTEGER ENCODE AZ64,
        broadcast_date TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        src_broadcast_network_name CHARACTER VARYING(255) ENCODE LZO,
        src_program_name CHARACTER VARYING(255) ENCODE LZO,
        src_market_break CHARACTER VARYING(255) ENCODE LZO,
        src_daypart_name CHARACTER VARYING(255) ENCODE LZO,
        src_playback_period_cd CHARACTER VARYING(200) ENCODE LZO,
        src_demographic_group CHARACTER VARYING(255) ENCODE LZO,
        mxm_source CHARACTER VARYING(255) ENCODE LZO,
        program_telecast_rpt_starttime CHARACTER VARYING(10) ENCODE LZO,
        program_telecast_rpt_endtime CHARACTER VARYING(10) ENCODE LZO,
        min_of_pgm_value BIGINT ENCODE AZ64,
        most_current_audience_avg_pct NUMERIC(20,2) ENCODE AZ64,
        most_current_us_audience_avg_proj_000 INTEGER ENCODE AZ64,
        most_current_nw_cvg_area_avg_pct NUMERIC(20,2) ENCODE AZ64,
        showdbid INTEGER ENCODE AZ64,
        title CHARACTER VARYING(1000) ENCODE LZO,
        subtitle CHARACTER VARYING(1000) ENCODE LZO,
        episodenumber CHARACTER VARYING(1000) ENCODE LZO,
        airdate DATE ENCODE AZ64,
        inpoint CHARACTER VARYING(50) ENCODE LZO,
        outpoint CHARACTER VARYING(50) ENCODE LZO,
        inpoint_24hr_est TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        modified_inpoint TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        modified_outpoint TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        segmenttype CHARACTER VARYING(200) ENCODE LZO,
        COMMENT CHARACTER VARYING(6000) ENCODE LZO,
        matchtype CHARACTER VARYING(500) ENCODE LZO,
        talentactions CHARACTER VARYING(1000) ENCODE LZO,
        move CHARACTER VARYING(200) ENCODE LZO,
        finishtype CHARACTER VARYING(500) ENCODE LZO,
        recorddate DATE ENCODE AZ64,
        fileid CHARACTER VARYING(200) ENCODE LZO,
        duration CHARACTER VARYING(200) ENCODE LZO,
        announcers CHARACTER VARYING(1000) ENCODE LZO,
        matchtitle CHARACTER VARYING(500) ENCODE LZO,
        venuelocation CHARACTER VARYING(100) ENCODE LZO,
        venuename CHARACTER VARYING(200) ENCODE LZO,
        issegmentmarker CHARACTER VARYING(200) ENCODE LZO,
        logentrydbid BIGINT ENCODE AZ64,
        logentryguid CHARACTER VARYING(256) ENCODE LZO,
        loggername CHARACTER VARYING(100) ENCODE LZO,
        logname CHARACTER VARYING(200) ENCODE LZO,
        masterclipid CHARACTER VARYING(2000) ENCODE LZO,
        modifieddatetime TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        networkassetid CHARACTER VARYING(100) ENCODE LZO,
        sponsors CHARACTER VARYING(256) ENCODE LZO,
        weapon CHARACTER VARYING(256) ENCODE LZO,
        season CHARACTER VARYING(100) ENCODE LZO,
        source_ffed_name CHARACTER VARYING(1000) ENCODE LZO,
        etl_batch_id CHARACTER VARYING(26) ENCODE LZO,
        etl_insert_user_id CHARACTER VARYING(15) ENCODE LZO,
        etl_insert_rec_dttm TIMESTAMP WITH TIME ZONE ENCODE AZ64,
        etl_update_user_id CHARACTER VARYING(1) ENCODE LZO,
        etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        additionaltalent CHARACTER VARYING(65535) ENCODE LZO
    );
COMMENT ON TABLE fds_nl.rpt_nl_daily_minxmin_lite_log_ratings
IS
    'Minute By Minture Ratings joining with Lite Log Report table';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.broadcast_date_id
IS
    'Broadcast Date ID field';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.broadcast_date
IS
    'The date on when the program is broadcasted';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.src_broadcast_network_name
IS
    'Broadcast Network Channel Name';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.src_program_name
IS
    'Name of the Program';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.src_market_break
IS
    'Identified Market break category by the broadcast network';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.src_daypart_name
IS
    'Name of the day part when program telecasted';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.src_playback_period_cd
IS
    'A comma separated list of data streams.';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.src_demographic_group
IS
    'A comma separated list of demographic groups (e.g. Females 18 to 49 and Males 18 - 24 input as F18-49,M18-24).'
    ;
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.mxm_source
IS
    'The source information for each feed type available for the minute by minute ratings';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.program_telecast_rpt_starttime
IS
    'Program Telecast Report Start Time';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.program_telecast_rpt_endtime
IS
    'Program Telecast Report End Time';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.min_of_pgm_value
IS
    'Metrics for Minute of program value';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.most_current_audience_avg_pct
IS
    'Metrics for most current audience average percentage value';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.most_current_us_audience_avg_proj_000
IS
    'Metrics for most current US audience average projection in thousands';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.most_current_nw_cvg_area_avg_pct
IS
    'Metrics for most current network coverage areas average percentage';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.showdbid
IS
    'Show DB ID to uniquely identify the lite log of the program telecasted at a time';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.title
IS
    'Title of the Program like NXT, RAW, SmackDown, etc.';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.subtitle
IS
    'Subtitle of the program';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.episodenumber
IS
    'Episode Number';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.airdate
IS
    'The date on when the program aired';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.inpoint
IS
    'The starting time of the particular segment';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.outpoint
IS
    'The end time of the particular segment';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.inpoint_24hr_est
IS
    'The starting time of the particular segment converted to 24Hr Clock';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.modified_inpoint
IS
    'Modified starting time of the particular segment by round it off to closest minute';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.modified_outpoint
IS
    'Modified end time of the particular segment by round it off to closest minute';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.segmenttype
IS
    'Type of Segment';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.comment
IS
    'The comment about the segment';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.matchtype
IS
    'Type of the Match';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.talentactions
IS
    'Describing the actions of talents';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.move
IS
    'Describing the move';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.finishtype
IS
    'The type of finish';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.recorddate
IS
    'The date on when the program recorded';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.fileid
IS
    'The File ID field';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.duration
IS
    'Duration of the Segment';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.announcers
IS
    'Announcers of the program';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.matchtitle
IS
    'Title of the match';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.venuelocation
IS
    'Venue Location of the Program';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.venuename
IS
    'Venue Name of the Program';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.issegmentmarker
IS
    'Segment Marker';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.logentrydbid
IS
    'Log Entry DB ID Field';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.logentryguid
IS
    'Log Entry Guide';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.loggername
IS
    'Name of the Logger';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.logname
IS
    'Log Name';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.masterclipid
IS
    'Master Clip ID';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.modifieddatetime
IS
    'Modified Date Time';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.networkassetid
IS
    'Network Asset ID';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.sponsors
IS
    'Sponsors of the Program';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.weapon
IS
    'Weapons used';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.season
IS
    'Season details';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.source_ffed_name
IS
    'source_ffed_name';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.etl_batch_id
IS
    'Unique ID of DBT Job used to insert the record';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.etl_insert_user_id
IS
    'Unique ID of the DBT user that was used to insert the record';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.etl_insert_rec_dttm
IS
    'Date Time information on when the DBT inserted the record';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.etl_update_user_id
IS
    'Unique ID of the DBT user which was used to update the record manually';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_minxmin_lite_log_ratings.etl_update_rec_dttm
IS
    'Date Time information on when the record was updated';