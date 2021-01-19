CREATE VIEW
    VW_AGGR_NL_QUARTERLY_WWE_LIVE_COMMERCIAL_RATINGS 
    (
        broadcast_quarter COMMENT 'Broadcast Quarter name',
        broadcast_year COMMENT 'Broadcast  year',
        src_broadcast_network_id COMMENT 'A unique numerical identifier for an individual programming originator.' ,
        src_playback_period_cd COMMENT 'A comma separated list of data streams. Time-shifted viewing from DVR Playback or On-demand content with the same commercial load.' ,
        src_demographic_group COMMENT 'A comma separated list of demographic groups (e.g. Females 18 to 49 and Males 18 - 24 input as F18-49,M18-24).',
        src_program_id COMMENT 'A unique numerical identifier for an individual program name. ' ,
        natl_comm_clockmts_avg_audience_proj_000 COMMENT  'National Commercial Clock Minute Average Audience Projection (000) (The projected number of households tuned or persons viewing the average qualified commercial minute of the selected program within the total U.S., expressed in thousands.)',
        natl_comm_clockmts_avg_audience_proj_pct COMMENT  'National Commercial Clock Minute Average Audience Percentage (The percentage of the target demographic viewing the average qualified commercial minute of the selected program within the total U.S.)',
        natl_comm_clockmts_cvg_area_avg_audience_proj_pct COMMENT   'National Commercial Clock Minute Coverage Area Average Audience Percent (The percentage of the target demographic viewing the average qualified commercial minute of a selected program within a network’s coverage area.)',
        tot_viewing_minutes COMMENT 'Derived Average Viewing Hours in minutes '
		) AS
select  broadcast_quarter_nm as broadcast_quarter, broadcast_year, 
src_broadcast_network_id, src_playback_period_cd, src_demographic_group, src_program_id,
--Duration Weighted Averages are taking for natl_comm_clockmts_avg_audience_proj_000, natl_comm_clockmts_avg_audience_proj_pct and --natl_comm_clockmts_cvg_area_avg_audience_proj_pct here..
(sum(natl_comm_clockmts_avg_audience_proj_000 * natl_comm_clockmts_duration)/
nullif(sum(nvl2(natl_comm_clockmts_avg_audience_proj_000, natl_comm_clockmts_duration, null)), 0)) as natl_comm_clockmts_avg_audience_proj_000,
(sum(natl_comm_clockmts_avg_audience_proj_pct * natl_comm_clockmts_duration)/
nullif(sum(nvl2(natl_comm_clockmts_avg_audience_proj_pct, natl_comm_clockmts_duration, null)), 0)) as natl_comm_clockmts_avg_audience_proj_pct,
(sum(natl_comm_clockmts_cvg_area_avg_audience_proj_pct * natl_comm_clockmts_duration)/
nullif(sum(nvl2(natl_comm_clockmts_cvg_area_avg_audience_proj_pct, natl_comm_clockmts_duration, null)), 0))
as natl_comm_clockmts_cvg_area_avg_audience_proj_pct, sum(avg_viewing_hours_units) as tot_viewing_minutes
from  rpt_nl_daily_wwe_live_commercial_ratings
group by 1,2,3,4,5,6