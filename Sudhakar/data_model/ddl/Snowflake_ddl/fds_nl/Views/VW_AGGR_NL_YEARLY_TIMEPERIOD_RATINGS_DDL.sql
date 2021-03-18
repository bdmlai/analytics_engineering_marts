CREATE VIEW
    VW_AGGR_NL_YEARLY_TIMEPERIOD_RATINGS
    (
        rpt_year,
        broadcast_network_name,
        src_daypart_name,
        src_playback_period_cd,
        src_demographic_group,
        avg_audience_proj_000,
        avg_audience_pct,
        avg_pct_nw_cvg_area
    ) AS
	SELECT  b.cal_year    as rpt_year, 
        c.broadcast_network_name as broadcast_network_name,
        d.src_daypart_name as src_daypart_name,
        src_playback_period_cd,
        src_demographic_group,
        avg(avg_audience_proj_000) as avg_audience_proj_000,
        avg(avg_audience_pct) as avg_audience_pct,
        avg(avg_pct_nw_cvg_area) as avg_pct_nw_cvg_area      
FROM      fds_nl.fact_nl_timeperiod_viewership_ratings a
LEFT JOIN  cdm.dim_date  b on a.rpt_startdate_id = b.dim_date_id
LEFT JOIN  fds_nl.dim_nl_broadcast_network  c on a.dim_nl_broadcast_network_id = c.dim_nl_broadcast_network_id
LEFT JOIN  fds_nl.dim_nl_daypart   d on a.dim_nl_daypart_id = d.dim_nl_daypart_id
GROUP BY  1,2,3,4,5