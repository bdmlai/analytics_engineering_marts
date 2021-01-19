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
    fds_nl.rpt_kntr_schedule_vh_data
    (
        dim_date_id INTEGER ENCODE AZ64,
        broadcast_date CHARACTER VARYING(30) ENCODE LZO,
        src_weekday CHARACTER VARYING(20) ENCODE LZO,
        month_name CHARACTER VARYING(11) ENCODE LZO,
        month_num INTEGER ENCODE AZ64,
        modified_month CHARACTER VARYING(32) ENCODE LZO,
        YEAR INTEGER ENCODE AZ64,
        region CHARACTER VARYING(256) ENCODE LZO,
        src_country CHARACTER VARYING(50) ENCODE LZO,
        broadcast_network_prem_type CHARACTER VARYING(30) ENCODE LZO,
        src_channel CHARACTER VARYING(70) ENCODE LZO,
        demographic_type CHARACTER VARYING(40) ENCODE LZO,
        demographic_group_name CHARACTER VARYING(40) ENCODE LZO,
        src_series CHARACTER VARYING(150) ENCODE LZO,
        series_episode_name CHARACTER VARYING(256) ENCODE LZO,
        series_episode_num CHARACTER VARYING(30) ENCODE LZO,
        series_name CHARACTER VARYING(100) ENCODE LZO,
        series_type CHARACTER VARYING(30) ENCODE LZO,
        start_time CHARACTER VARYING(30) ENCODE LZO,
        end_time CHARACTER VARYING(30) ENCODE LZO,
        duration_mins INTEGER ENCODE AZ64,
        hd_flag CHARACTER VARYING(5) ENCODE LZO,
        week_start_date DATE ENCODE AZ64,
        start_time_modified CHARACTER VARYING(57) ENCODE LZO,
        channel_1 CHARACTER VARYING(70) ENCODE LZO,
        program_1 CHARACTER VARYING(100) ENCODE LZO,
        rat_value NUMERIC(38,18) ENCODE AZ64,
        viewing_hours NUMERIC(38,4) ENCODE AZ64,
        aud NUMERIC(38,2) ENCODE AZ64,
        regional_viewing_hours NUMERIC(38,4) ENCODE AZ64,
        etl_batch_id CHARACTER VARYING(26) ENCODE LZO,
        etl_insert_user_id CHARACTER VARYING(15) ENCODE LZO,
        etl_insert_rec_dttm TIMESTAMP WITH TIME ZONE ENCODE AZ64,
        etl_update_user_id CHARACTER VARYING(1) ENCODE LZO,
        etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64
    )
    DISTSTYLE KEY DISTKEY
    (
        demographic_group_name
    );
COMMENT ON TABLE fds_nl.rpt_kntr_schedule_vh_data
IS
   'WWE Program Schedule Viewing Hours Report table'
    ;
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.dim_date_id
IS
    'The broadcast date ID field';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.broadcast_date
IS
    'The date on when the WWE program broadcasted';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.src_weekday
IS
    'The day on when the WWE program broadcasted';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.month_name
IS
    'The month name on when the WWE program broadcasted';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.month_num
IS
    'The month number on when the WWE program broadcasted';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.modified_month
IS
    'Derived from broadcast_date; day part of broadcast_date replaced with ''01''';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.year
IS
    'The broadcast year when the WWE program broadcasted';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.region
IS
    'The region of country from where the WWE program is telecasted';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.src_country
IS
    'The country from where the WWE program is telecasted';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.broadcast_network_prem_type
IS
    'Indicates whether the channel is Pay / Free To Air';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.src_channel
IS
    'The channel name in which WWE program telecasted';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.demographic_type
IS
    'The type of demographic group watched the WWE program';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.demographic_group_name
IS
    'The demographic group name who watched the WWE program';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.src_series
IS
    'Program name broadcasted on a partucular broadcast network (channel)';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.series_episode_name
IS
    'Name of the content broadcast';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.series_episode_num
IS
    'No.of episodes broadcast';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.series_name
IS
    'Indicates WWE series name';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.series_type
IS
    'Indicates WWE series type';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.start_time
IS
    'Start Time of the WWE Program';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.end_time
IS
    'End Time of the WWE Program';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.duration_mins
IS
    'The duration of WWE Program in minutes';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.hd_flag
IS
    'Indicates whether the channel is HD or not';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.week_start_date
IS
    'Calendar Year Week Start Date based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.start_time_modified
IS
    'The Start Time of WWE Program rounded off to the closest hour in timestamp format';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.channel_1
IS
    'The value will be ''Others'', if the channel is broadcasting for more than an year and percentage of viewership of the channel (compared to total viewership of country) is <=0.01'
    ;
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.program_1
IS
    'The value will be ''Others'' for the programs other than ''RAW'', ''SMACKDOWN'', ''NXT'', ''PPV'', ''SUNDAY DHAMAAL'', ''SATURDAY NIGHT'', ''TOTAL BELLAS'' and ''TOTAL DIVAS'''
    ;
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.rat_value
IS
    'The average rating value of the WWE program';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.viewing_hours
IS
    'The viewing hours of WWE program by specified demographic group';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.aud
IS
    'The average audience who watched the WWE Program';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.regional_viewing_hours
IS
    'The regional viewing hours of the demographic group on monthly-basis';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.etl_batch_id
IS
    'Unique ID of DBT Job used to insert the record';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.etl_insert_user_id
IS
    'Unique ID of the DBT user that was used to insert the record';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.etl_insert_rec_dttm
IS
    'Date Time information on when the DBT inserted the record';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.etl_update_user_id
IS
    'Unique ID of the DBT user which was used to update the record manually';
COMMENT ON COLUMN fds_nl.rpt_kntr_schedule_vh_data.etl_update_rec_dttm
IS
    'Date Time information on when the record was updated';