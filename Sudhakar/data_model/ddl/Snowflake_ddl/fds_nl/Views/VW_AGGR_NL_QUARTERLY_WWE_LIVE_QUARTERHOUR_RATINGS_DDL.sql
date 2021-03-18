CREATE VIEW
    VW_AGGR_NL_QUARTERLY_WWE_LIVE_QUARTERHOUR_RATINGS
    (
        broadcast_quarter,
        broadcast_year,
        src_broadcast_network_id,
        src_playback_period_cd,
        src_demographic_group,
        src_program_id,
        interval_starttime,
        interval_endtime,
        avg_audience_proj_000,
        avg_audience_pct,
        avg_pct_nw_cvg_area,
        tot_viewing_minutes
    ) AS
	SELECT broadcast_quarter_nm broadcast_quarter, 
       broadcast_year, 
       src_broadcast_network_id,
       src_playback_period_cd,
       src_demographic_group,
       src_program_id,
       interval_starttime,
       interval_endtime,
--Duration Weighted Averages are taking for avg_audience_proj_000, avg_audience_pct and avg_pct_nw_cvg_area here..
(sum(avg_audience_proj_000 * interval_duration)/nullif(sum(nvl2(avg_audience_proj_000, interval_duration, null)), 0)) as avg_audience_proj_000,
(sum(avg_audience_pct * interval_duration)/nullif(sum(nvl2(avg_audience_pct, interval_duration, null)), 0)) as avg_audience_pct,
(sum(avg_pct_nw_cvg_area * interval_duration)/nullif(sum(nvl2(avg_pct_nw_cvg_area, interval_duration, null)), 0)) as avg_pct_nw_cvg_area, 
sum(avg_viewing_hours_units) as tot_viewing_minutes
FROM rpt_nl_daily_wwe_live_quarterhour_ratings
GROUP BY  1,2,3,4,5,6,7,8 ;