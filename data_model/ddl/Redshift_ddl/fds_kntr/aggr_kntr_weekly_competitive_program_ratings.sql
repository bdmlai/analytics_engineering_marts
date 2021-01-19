/*
************************************************************************************ 
    Date: 9/03/2020
    Version: 1.0
    Description: First Draft
    Contributor: Hima Dasan
    Adl' notes:  
    updated comments
**************************************************************************************/
/* date      | by      |    Details   */

CREATE TABLE
    fds_kntr.aggr_kntr_weekly_competitive_program_ratings
    (
        week_start_date DATE ENCODE AZ64,
        week_end_date DATE ENCODE AZ64,
        week_num SMALLINT ENCODE AZ64,
        MONTH CHARACTER VARYING(11) ENCODE LZO,
        quarter CHARACTER VARYING(12) ENCODE LZO,
        YEAR INTEGER ENCODE AZ64,
        src_country CHARACTER VARYING(50) ENCODE LZO,
        src_channel CHARACTER VARYING(70) ENCODE LZO,
        src_property CHARACTER VARYING(100) ENCODE LZO,
        demographic CHARACTER VARYING(255) ENCODE LZO,
        hd_flag CHARACTER VARYING(3) ENCODE LZO,
        total_duration_mins NUMERIC(38,2) ENCODE AZ64,
        duration_hours NUMERIC(38,5) ENCODE AZ64,
        rat_value NUMERIC(38,4) ENCODE AZ64,
        viewing_hours NUMERIC(38,8) ENCODE AZ64,
        telecasts_count BIGINT ENCODE AZ64,
        weekly_cumulative_audience NUMERIC(38,3) ENCODE AZ64,
        etl_batch_id CHARACTER VARYING(26) ENCODE LZO,
        etl_insert_user_id CHARACTER VARYING(15) ENCODE LZO,
        etl_insert_rec_dttm TIMESTAMP WITH TIME ZONE ENCODE AZ64,
        etl_update_user_id CHARACTER VARYING(1) ENCODE LZO,
        etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64
    );

COMMENT ON TABLE fds_kntr.aggr_kntr_weekly_competitive_program_ratings
IS
   "Description : Competitive Program Ratings Weekly Aggregate Table consist of rating details of competitive programs referencing from Annual Profile Table on weekly-basis"
   ;
COMMENT ON COLUMN fds_kntr.aggr_kntr_weekly_competitive_program_ratings.week_start_date
IS
    'Calendar Year Week Start Date based on Broadcast Date';
COMMENT ON COLUMN fds_kntr.aggr_kntr_weekly_competitive_program_ratings.week_end_date
IS
    'Calendar Year Week End Date based on Broadcast Date';
COMMENT ON COLUMN fds_kntr.aggr_kntr_weekly_competitive_program_ratings.week_num
IS
    'Calendar Year Week Number based on Broadcast Date';
COMMENT ON COLUMN fds_kntr.aggr_kntr_weekly_competitive_program_ratings.month
IS
    'Abbreviated Month Name based on week start date';
COMMENT ON COLUMN fds_kntr.aggr_kntr_weekly_competitive_program_ratings.quarter
IS
    'Abbreviated Quarter Name based on week start date';
COMMENT ON COLUMN fds_kntr.aggr_kntr_weekly_competitive_program_ratings.year
IS
    'Year based on week start date';
COMMENT ON COLUMN fds_kntr.aggr_kntr_weekly_competitive_program_ratings.src_country
IS
    'country name where competitive program telecasted';
COMMENT ON COLUMN fds_kntr.aggr_kntr_weekly_competitive_program_ratings.src_channel
IS
    'The channel name in which competitive program telecasted';
COMMENT ON COLUMN fds_kntr.aggr_kntr_weekly_competitive_program_ratings.src_property
IS
    'The competitive program which is telecasted';
COMMENT ON COLUMN fds_kntr.aggr_kntr_weekly_competitive_program_ratings.demographic
IS
    'The demographic group who watched the competitive program';
COMMENT ON COLUMN fds_kntr.aggr_kntr_weekly_competitive_program_ratings.hd_flag
IS
    'Indicates whether the channel is HD or not';
COMMENT ON COLUMN fds_kntr.aggr_kntr_weekly_competitive_program_ratings.total_duration_mins
IS
    'The total duration of the competitive program in minutes';
COMMENT ON COLUMN fds_kntr.aggr_kntr_weekly_competitive_program_ratings.duration_hours
IS
    'The total duration of the competitive program in hours';
COMMENT ON COLUMN fds_kntr.aggr_kntr_weekly_competitive_program_ratings.rat_value
IS
    'The rating value of the competitive program';
COMMENT ON COLUMN fds_kntr.aggr_kntr_weekly_competitive_program_ratings.viewing_hours
IS
    'The viewing hours of competitive program by specified demographic group';
COMMENT ON COLUMN fds_kntr.aggr_kntr_weekly_competitive_program_ratings.telecasts_count
IS
    'Count of Telecasts of competitive program';
COMMENT ON COLUMN fds_kntr.aggr_kntr_weekly_competitive_program_ratings.weekly_cumulative_audience
IS
    'The weekly cumulative Audience who watched the competitive program';
COMMENT ON COLUMN fds_kntr.aggr_kntr_weekly_competitive_program_ratings.etl_batch_id
IS
    'Unique ID of DBT Job used to insert the record';
COMMENT ON COLUMN fds_kntr.aggr_kntr_weekly_competitive_program_ratings.etl_insert_user_id
IS
    'Unique ID of the DBT user that was used to insert the record';
COMMENT ON COLUMN fds_kntr.aggr_kntr_weekly_competitive_program_ratings.etl_insert_rec_dttm
IS
    'Date Time information on when the DBT inserted the record';
COMMENT ON COLUMN fds_kntr.aggr_kntr_weekly_competitive_program_ratings.etl_update_user_id
IS
    'Unique ID of the DBT user which was used to update the record manually';
COMMENT ON COLUMN fds_kntr.aggr_kntr_weekly_competitive_program_ratings.etl_update_rec_dttm
IS
    'Date Time information on when the record was updated';