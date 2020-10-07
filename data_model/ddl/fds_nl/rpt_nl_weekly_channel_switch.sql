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
    fds_nl.rpt_nl_weekly_channel_switch
    (
        broadcast_date TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        coverage_area CHARACTER VARYING(255) ENCODE LZO,
        src_market_break CHARACTER VARYING(255) ENCODE LZO,
        src_broadcast_network_name CHARACTER VARYING(255) ENCODE LZO,
        src_demographic_group CHARACTER VARYING(255) ENCODE LZO,
        time_minute CHARACTER VARYING(10) ENCODE LZO,
        mc_us_aa000 INTEGER ENCODE AZ64,
        absolute_set_off_off_air NUMERIC(35,6) ENCODE AZ64,
        absolute_stay NUMERIC(35,6) ENCODE AZ64,
        stay_percent NUMERIC(38,6) ENCODE AZ64,
        absolute_switch NUMERIC(38,6) ENCODE AZ64,
        switch_percent NUMERIC(38,6) ENCODE AZ64,
        switch_percent_rank BIGINT ENCODE AZ64,
        etl_batch_id CHARACTER VARYING(26) ENCODE LZO,
        etl_insert_user_id CHARACTER VARYING(15) ENCODE LZO,
        etl_insert_rec_dttm TIMESTAMP WITH TIME ZONE ENCODE AZ64,
        etl_update_user_id CHARACTER VARYING(1) ENCODE LZO,
        etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64
    );
COMMENT ON TABLE fds_nl.rpt_nl_weekly_channel_switch
IS
    'Weekly channel switch report table';
COMMENT ON COLUMN fds_nl.rpt_nl_weekly_channel_switch.broadcast_date
IS
    'Broadcast date for the given channel';
COMMENT ON COLUMN fds_nl.rpt_nl_weekly_channel_switch.coverage_area
IS
    'NPOWER- Coverage Area';
COMMENT ON COLUMN fds_nl.rpt_nl_weekly_channel_switch.src_market_break
IS
    'Identified Marcket break category by the broadcast nework';
COMMENT ON COLUMN fds_nl.rpt_nl_weekly_channel_switch.src_broadcast_network_name
IS
    'Viewing source of the network channel';
COMMENT ON COLUMN fds_nl.rpt_nl_weekly_channel_switch.src_demographic_group
IS
    'A comma separated list of demographic groups (e.g. Females 18 to 49 and Males 18 - 24 input as F18-49,M18-24).'
    ;
COMMENT ON COLUMN fds_nl.rpt_nl_weekly_channel_switch.time_minute
IS
    'minute of measurement the intervel of switching beahiour is measured';
COMMENT ON COLUMN fds_nl.rpt_nl_weekly_channel_switch.mc_us_aa000
IS
    'Metrics for most current US audience average projection in thousants';
COMMENT ON COLUMN fds_nl.rpt_nl_weekly_channel_switch.absolute_stay
IS
    'unique viewers who stayed in the same network';
COMMENT ON COLUMN fds_nl.rpt_nl_weekly_channel_switch.stay_percent
IS
    'percentage value of viewers stayed in same network';
COMMENT ON COLUMN fds_nl.rpt_nl_weekly_channel_switch.absolute_switch
IS
    'unique viewers who swicthed to different network';
COMMENT ON COLUMN fds_nl.rpt_nl_weekly_channel_switch.switch_percent
IS
    'percentage value of unique viewers who swicthed to different network';
COMMENT ON COLUMN fds_nl.rpt_nl_weekly_channel_switch.switch_percent_rank
IS
    'ranking based on percentage switch';