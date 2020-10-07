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
/* 8/06/2020 | Remya K Nair | Added src_series_name column */

CREATE TABLE
    fds_nl.rpt_nl_daily_wwe_program_ratings
    (
        broadcast_date_id INTEGER ENCODE LZO,
        broadcast_date DATE ENCODE LZO,
        broadcast_cal_week_begin_date DATE ENCODE LZO,
        broadcast_cal_week_end_date DATE ENCODE LZO,
        broadcast_cal_week_num SMALLINT ENCODE LZO,
        broadcast_cal_month_num SMALLINT ENCODE LZO,
        broadcast_cal_month_nm CHARACTER VARYING(20) ENCODE LZO,
        broadcast_cal_quarter CHARACTER VARYING(20) ENCODE LZO,
        broadcast_cal_year SMALLINT ENCODE LZO,
        broadcast_fin_week_begin_date DATE ENCODE LZO,
        broadcast_fin_week_end_date DATE ENCODE LZO,
        broadcast_fin_week_num BIGINT ENCODE LZO,
        broadcast_fin_month_num BIGINT ENCODE LZO,
        broadcast_fin_month_nm CHARACTER VARYING(20) ENCODE LZO,
        broadcast_fin_year BIGINT ENCODE LZO,
        src_broadcast_network_id BIGINT ENCODE LZO,
        broadcast_network_name CHARACTER VARYING(255) ENCODE LZO,
        src_playback_period_cd CHARACTER VARYING(255) ENCODE LZO,
        src_demographic_group CHARACTER VARYING(255) ENCODE LZO,
        src_program_id BIGINT ENCODE BYTEDICT,
        src_series_name CHARACTER VARYING(255) ENCODE LZO,
        src_daypart_cd CHARACTER VARYING(255) ENCODE LZO,
        src_daypart_name CHARACTER VARYING(255) ENCODE LZO,
        program_telecast_rpt_starttime CHARACTER VARYING(255) ENCODE LZO,
        program_telecast_rpt_endtime CHARACTER VARYING(255) ENCODE LZO,
        src_total_duration BIGINT ENCODE BYTEDICT,
        avg_audience_proj_000 BIGINT ENCODE AZ64,
        avg_audience_pct NUMERIC(20,2) ENCODE BYTEDICT,
        avg_audience_pct_nw_cvg_area NUMERIC(20,2) ENCODE BYTEDICT,
        viewing_minutes_units BIGINT ENCODE AZ64,
        etl_batch_id CHARACTER VARYING(100) ENCODE LZO,
        etl_insert_user_id CHARACTER VARYING(100) ENCODE LZO,
        etl_insert_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        etl_update_user_id CHARACTER VARYING(50) ENCODE LZO,
        etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        broadcast_fin_quarter CHARACTER VARYING(16383) ENCODE LZO
    );
COMMENT ON TABLE fds_nl.rpt_nl_daily_wwe_program_ratings
IS
    'WWE Live Program Ratings Daily Report table';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_date_id
IS
    'Broadcast Date ID Field';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_date
IS
    'Derived dates based on the viewing period; before 6 am morning hours is the previous date broadcast hour'
    ;
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_cal_week_begin_date
IS
    'Calendar Year Week Begin Date based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_cal_week_end_date
IS
    'Calendar Year Week End Date based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_cal_week_num
IS
    'Calendar Year Week Number based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_cal_month_num
IS
    'Calendar Year Month Number based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_cal_month_nm
IS
    'Calendar Year Month based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_cal_quarter
IS
    'Calendar Year Quarter based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_cal_year
IS
    'Calendar Year based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_fin_week_begin_date
IS
    'Financial Year Week Begin Date based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_fin_week_end_date
IS
    'Financial Year Week End Date based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_fin_week_num
IS
    'Financial Year Week Number based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_fin_month_num
IS
    'Financial Year Month Number based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_fin_month_nm
IS
    'Financial Year Month based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_fin_year
IS
    'Financial Year based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.src_broadcast_network_id
IS
    'A unique numerical identifier for an individual programming originator.';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_network_name
IS
    'Broadcast netowrk Name or the channel name or view source name';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.src_playback_period_cd
IS
    'A comma separated list of data streams.';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.src_demographic_group
IS
    'A comma separated list of demographic groups (e.g. Females 18 to 49 and Males 18 - 24 input as F18-49,M18-24).'
    ;
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.src_program_id
IS
    'A unique numerical identifier for an individual program nam.';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.src_series_name
IS
    'A unique numerical identifier for an individual program name.';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.src_daypart_cd
IS
    'A unique character identifier for an individual daypart';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.src_daypart_name
IS
    'A unique character identifier for an individual daypart description';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.program_telecast_rpt_starttime
IS
    'The start time of the program telecast (HH:MM).';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.program_telecast_rpt_endtime
IS
    'The end time of the program telecast (HH:MM).';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.src_total_duration
IS
    'The duration of the program/telecast airing (minutes).';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.avg_audience_proj_000
IS
    'Total U.S. Average Audience Projection (000) (The projected number of households tuned or persons viewing a program/originator/daypart during the average minute, expressed in thousands.)'
    ;
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.avg_audience_pct
IS
    'Total U.S. Average Audience Percentage (The percentage of the target demographic viewing the average minute of the selected program or time period within the total U.S.)'
    ;
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.avg_audience_pct_nw_cvg_area
IS
    'Coverage Area Average Audience Percent (The percentage of the target demographic viewing the average minute of a selected program or time period within a network’s coverage area.)'
    ;
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.viewing_minutes_units
IS
    'Derived Average Viewing Hours in minutes';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.etl_batch_id
IS
    'Unique ID of DBT Job used to insert the record';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.etl_insert_user_id
IS
    'Unique ID of the DBT user that was used to insert the record';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.etl_insert_rec_dttm
IS
    'Date Time information on when the DBT inserted the record';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.etl_update_user_id
IS
    'Unique ID of the DBT user which was used to update the record manually';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.etl_update_rec_dttm
IS
    'Date Time information on when the record was updated';