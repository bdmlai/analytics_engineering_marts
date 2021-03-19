
CREATE VIEW
    vw_aggr_nl_quarterly_wwe_live_program_ratings COMMENT = 'A view to display various dimensions and metrics of wwe live programs on quarterly-basis'
    (
        broadcast_quarter COMMENT 'Broadcast Calendar Quarter',
        broadcast_year COMMENT 'Broadcast Calendar Year',
        src_broadcast_network_id COMMENT 'A unique numerical identifier for an individual programming originator.' ,
        src_playback_period_cd COMMENT 'A comma separated list of data streams. Time-shifted viewing from DVR Playback or On-demand content with the same commercial load.• Live (Live - Includes viewing that occurred during the live airing).• Live+SD (Live + Same Day -Includes all playback that occurred within the same day of the liveairing).• Live+3 (Live + 3 Days - Includes all playback that occurred within three days of the live airing).• Live+7 (Live + 7 Days - Includes all playback that occurred within seven days of the live airing).',
        src_demographic_group COMMENT 'A comma separated list of demographic groups (e.g. Females 18 to 49 and Males 18 - 24 input as F18-49,M18-24).',
        src_program_id COMMENT  'A unique numerical identifier for an individual program name',
        src_series_name COMMENT 'Series name or program name provided by the source',
        avg_audience_proj_000 COMMENT 'Total U.S. Average Audience Projection (000) (The projected number of households tuned or persons viewing a program/originator/daypart during the average minute, expressed in thousands.)',
        avg_audience_pct COMMENT 'Total U.S. Average Audience Percentage (The percentage of the target demographic viewing the average minute of the selected program or time period within the total U.S.)',
        avg_audience_pct_nw_cvg_area COMMENT  'Coverage Area Average Audience Percent (The percentage of the target demographic viewing the average minute of a selected program or time period within a network’s coverage area.)',
        tot_viewing_minutes COMMENT 'Derived Average Viewing Hours in minutes',
        number_of_airings COMMENT 'Number of airings'
    ) AS
SELECT broadcast_cal_quarter as broadcast_quarter, broadcast_cal_year as broadcast_year, 
src_broadcast_network_id, src_playback_period_cd, src_demographic_group, src_program_id,src_series_name,
--Duration Weighted Averages are taking here for avg_audience_proj_000, avg_audience_pct and avg_audience_pct_nw_cvg_area..
(sum(avg_audience_proj_000 * src_total_duration)/nullif(sum(nvl2(avg_audience_proj_000, src_total_duration, null)), 0)) as avg_audience_proj_000,
(sum(avg_audience_pct * src_total_duration)/nullif(sum(nvl2(avg_audience_pct, src_total_duration, null)), 0)) as avg_audience_pct,
(sum(avg_audience_pct_nw_cvg_area * src_total_duration)/nullif(sum(nvl2(avg_audience_pct_nw_cvg_area, src_total_duration, null)), 0)) as avg_audience_pct_nw_cvg_area, sum(viewing_minutes_units) as tot_viewing_minutes, count(*) as number_of_airings
FROM rpt_nl_daily_wwe_program_ratings
WHERE (src_broadcast_network_id, src_program_id)
   IN ((5, 296881), (5, 339681), (5, 436999), (81, 898521), (10433, 1000131))
GROUP BY 1,2,3,4,5,6,7 ;