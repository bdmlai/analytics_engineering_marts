/*
************************************************************************************ 
    Date: 6/12/2020
    Version: 1.0
    Description: First Draft
    Contributor: Rahul Chandran
    Adl' notes:  
    updated comments
**************************************************************************************/
/* date      | by      |    Details   */
/* 9/20/2020 | Remya K Nair | Added two columns - program_telecast_rpt_starttime and program_telecast_rpt_endtime                                            */


create table fds_nl.rpt_nl_daily_wwe_live_commercial_ratings
(
broadcast_date_id INTEGER ENCODE AZ64,
        broadcast_date TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        broadcast_month_num SMALLINT ENCODE AZ64,
        broadcast_month_nm CHARACTER VARYING(20) ENCODE ZSTD,
        broadcast_quarter_num SMALLINT ENCODE AZ64,
        broadcast_quarter_nm CHARACTER VARYING(20) ENCODE ZSTD,
        broadcast_year SMALLINT ENCODE AZ64,
        src_broadcast_network_id CHARACTER VARYING(200) ENCODE ZSTD,
        src_playback_period_cd CHARACTER VARYING(200) ENCODE ZSTD,
        src_demographic_group CHARACTER VARYING(255) ENCODE ZSTD,
        src_program_id CHARACTER VARYING(255) ENCODE ZSTD,
        avg_viewing_hours_units BIGINT ENCODE AZ64,
        natl_comm_clockmts_avg_audience_proj_000 INTEGER ENCODE AZ64,
        natl_comm_clockmts_avg_audience_proj_pct NUMERIC(20,2) ENCODE AZ64,
        natl_comm_clockmts_cvg_area_avg_audience_proj_pct NUMERIC(20,2) ENCODE AZ64,
        natl_comm_clockmts_duration INTEGER ENCODE AZ64,
        etl_batch_id CHARACTER VARYING(100) ENCODE LZO,
        etl_insert_user_id CHARACTER VARYING(100) ENCODE LZO,
        etl_insert_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        etl_update_user_id CHARACTER VARYING(50) ENCODE LZO,
        etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        program_telecast_rpt_starttime CHARACTER VARYING(10) ENCODE ZSTD,
        program_telecast_rpt_endtime   CHARACTER VARYING(10) ENCODE ZSTD
);

COMMENT ON TABLE fds_nl.rpt_nl_daily_wwe_live_commercial_ratings
IS
    'WWE Live Commercial Ratings Daily Report table';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_commercial_ratings.broadcast_date_id
IS
    'Broadcast Date ID field';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_commercial_ratings.broadcast_date
IS
    'Derived dates based on the viewing period; before 6 am morning hours is the previous date broadcast hour';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_commercial_ratings.broadcast_month_num
IS
    'Broadcast Month Number';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_commercial_ratings.broadcast_month_nm
IS
    'Broadcast Month abbr name';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_commercial_ratings.broadcast_quarter_num
IS
    'Broadcast Quarter number';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_commercial_ratings.broadcast_quarter_nm
IS
    'broadcast Quarter Name';	
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_commercial_ratings.broadcast_year
IS
    'Broadcast year';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_commercial_ratings.src_broadcast_network_id
IS
    'A unique numerical identifier for an individual programming originator.'
    ;
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_commercial_ratings.src_playback_period_cd
IS
    'A comma separated list of data streams. Time-shifted viewing from DVR Playback or On-demand content with the same commercial load.• Live (Live - Includes viewing that occurred during the live airing).• Live+SD (Live + Same Day -Includes all playback that occurred within the same day of the liveairing).• Live+3 (Live + 3 Days - Includes all playback that occurred within three days of the live airing).• Live+7 (Live + 7 Days - Includes all playback that occurred within seven days of the live airing).'
	 ;
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_commercial_ratings.src_demographic_group
IS
    'A comma separated list of demographic groups (e.g. Females 18 to 49 and Males 18 - 24 input as F18-49,M18-24).'
    ;
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_commercial_ratings.src_program_id
IS
    'A unique numerical identifier for an individual program name.'
    ;
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_commercial_ratings.avg_viewing_hours_units
IS
    'Derived Average Viewing Hours in minutes';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_commercial_ratings.natl_comm_clockmts_avg_audience_proj_000
IS
    'National Commercial Clock Minute Average 
Audience Projection (000) (The projected number of households tuned or persons viewing the average qualified commercial minute of the selected program within the total U.S., expressed in thousands.)'
    ;
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_commercial_ratings.natl_comm_clockmts_avg_audience_proj_pct
IS
    'National Commercial Clock Minute Average Audience Percentage (The percentage of the target demographic viewing the average qualified commercial minute of the selected program within the total U.S.)'
    ;
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_commercial_ratings.natl_comm_clockmts_cvg_area_avg_audience_proj_pct
IS
    'National Commercial Clock Minute Coverage Area Average Audience Percent (The percentage of the target demographic viewing the average qualified commercial minute of a selected program within a network’s coverage area.)'
    ;
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_commercial_ratings.natl_comm_clockmts_duration
IS
    'National Commerical Clock Minutes Duration (seconds) (The total number of seconds across qualified commercial minutes within a selected program.)' ;
 COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_commercial_ratings.program_telecast_rpt_starttime
IS
    'The start time of the program telecast (HH:MM)' ;
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_commercial_ratings.program_telecast_rpt_endtime
IS
    'The end time of the program telecast (HH:MM)' ;       