CREATE VIEW
    VW_AGGR_NL_MONTHLY_TIMEPERIOD_RATINGS
    (
        rpt_month_nm,
        rpt_year,
        broadcast_network_name,
        src_daypart_name,
        src_playback_period_cd,
        src_demographic_group,
        avg_audience_proj_000,
        avg_audience_pct,
        avg_pct_nw_cvg_area
    ) AS
SELECT
    b.mth_abbr_nm AS rpt_month_nm,
    b.cal_year    AS rpt_year,
    c.broadcast_network_name,
    d.src_daypart_name,
    a.src_playback_period_cd,
    a.src_demographic_group,
    AVG(a.avg_audience_proj_000) AS avg_audience_proj_000,
    AVG(a.avg_audience_pct)      AS avg_audience_pct,
    AVG(a.avg_pct_nw_cvg_area)   AS avg_pct_nw_cvg_area
FROM
    (((fact_nl_timeperiod_viewership_ratings a
LEFT JOIN
    cdm.dim_date b
ON
    ((
            a.rpt_startdate_id = b.dim_date_id)))
LEFT JOIN
    dim_nl_broadcast_network c
ON
    ((
            a.dim_nl_broadcast_network_id = c.dim_nl_broadcast_network_id)))
LEFT JOIN
    dim_nl_daypart d
ON
    ((
            a.dim_nl_daypart_id = d.dim_nl_daypart_id)))
GROUP BY
    b.mth_abbr_nm,
    b.cal_year,
    c.broadcast_network_name,
    d.src_daypart_name,
    a.src_playback_period_cd,
    a.src_demographic_group;