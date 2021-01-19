
CREATE VIEW
    VW_AGGR_NL_MONTHLY_WWE_LIVE_QUARTERHOUR_RATINGS COMMENT='A VIEW FOR QUARTER HOUR RATINGS'
    (
        broadcast_month COMMENT 'Broadcast Month name',
        broadcast_year COMMENT 'Broadcast  year',
        src_broadcast_network_id COMMENT 'A unique numerical identifier for an individual programming originator.',
        src_playback_period_cd COMMENT 'A comma separated list of data streams. Time-shifted viewing from DVR Playback or On-demand content with the same commercial load.',
        src_demographic_group COMMENT 'A comma separated list of demographic groups (e.g. Females 18 to 49 and Males 18 - 24 input as F18-49,M18-24).',
        src_program_id COMMENT 'A unique numerical identifier for an individual program name. ',
        interval_starttime COMMENT 'calcuated interval start time if it is quarter hour , every quarter start time will be profided',
        interval_endtime COMMENT 'calcuated interval end time if it is quarter hour , every quarter end time will be profided',
        avg_audience_proj_000 COMMENT 'Total U.S. Average Audience Projection (000) (The projected number of households tuned or persons viewing a program/originator/daypart during the average minute, expressed in thousands.)',
        avg_audience_pct COMMENT 'Total U.S. Average Audience Percentage (The percentage of the target demographic viewing the average minute of the selected program or time period within the total U.S.)',
        avg_pct_nw_cvg_area COMMENT 'Coverage Area Average Audience Percent (The percentage of the target demographic viewing the average minute of a selected program or time period within a network’s coverage area.)',
        tot_viewing_minutes  COMMENT 'Derived Average Viewing Hours in minutes'
    ) AS
SELECT broadcast_month_nm as broadcast_month, 
       broadcast_year, 
       src_broadcast_network_id,
       src_playback_period_cd,
       src_demographic_group,
       src_program_id,
       interval_starttime,
       interval_endtime,
(sum(avg_audience_proj_000 * interval_duration)/nullif(sum(nvl2(avg_audience_proj_000, interval_duration, null)), 0)) as avg_audience_proj_000,
(sum(avg_audience_pct * interval_duration)/nullif(sum(nvl2(avg_audience_pct, interval_duration, null)), 0)) as avg_audience_pct,
(sum(avg_pct_nw_cvg_area * interval_duration)/nullif(sum(nvl2(avg_pct_nw_cvg_area, interval_duration, null)), 0)) as avg_pct_nw_cvg_area, 
sum(avg_viewing_hours_units) as tot_viewing_minutes
FROM rpt_nl_daily_wwe_live_quarterhour_ratings
GROUP BY  1,2,3,4,5,6,7,8;