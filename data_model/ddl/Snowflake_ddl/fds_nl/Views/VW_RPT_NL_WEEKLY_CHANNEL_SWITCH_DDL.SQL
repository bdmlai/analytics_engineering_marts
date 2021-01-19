
CREATE VIEW
    VW_RPT_NL_WEEKLY_CHANNEL_SWITCH COMMENT='## Implementation Detail
* Date        : 06/19/2020
* Version     : 1.0
* TableName   : vw_rpt_nl_weekly_channel_switch
* Schema   : fds_nl
* Contributor : Hima Dasan
* Description : vw_rpt_nl_weekly_channel_switch view consists of absolute stay ,absolute switch and ranking based on switch for WWE, AEW and other wrestling programs (Weekly)

## Maintenance Log
* Date : 06/19/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of Phase 4b Project.'
    (
        broadcast_date COMMENT 'Broadcast date for the given channel',
        coverage_area COMMENT 'NPOWER- Coverage Area',
        src_market_break COMMENT 'Identified Marcket break category by the broadcast nework',
        src_demographic_group COMMENT 'A comma separated list of demographic groups (e.g. Females 18 to 49 and Males 18 - 24 input as F18-49,M18-24).',
        src_broadcast_network_name COMMENT 'Viewing source of the network channel',
        time_minute COMMENT 'minute of measurement the intervel of switching beahiour is measured',
        mc_us_aa000 COMMENT 'Metrics for most current US audience average projection in thousants',
        absolute_stay COMMENT 'unique viewers who stayed in the same network',
        stay_percent COMMENT 'percentage value of viewers stayed in same network',
        absolute_switch COMMENT 'unique viewers who swicthed to different network',
        switch_percent COMMENT 'percentage value of unique viewers who swicthed to different network',
        switch_percent_rank COMMENT 'ranking based on percentage switch'
    ) AS
SELECT
    a.broadcast_date,
    a.coverage_area,
    a.src_market_break,
    a.src_demographic_group,
    a.src_broadcast_network_name,
    a.time_minute,
    a.mc_us_aa000,
    a.absolute_stay,
    a.stay_percent,
    a.absolute_switch,
    a.switch_percent,
    a.switch_percent_rank
FROM
    rpt_nl_weekly_channel_switch a;