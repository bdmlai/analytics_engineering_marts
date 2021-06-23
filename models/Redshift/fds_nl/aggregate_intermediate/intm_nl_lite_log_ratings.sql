{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    broadcast_date_id,
    broadcast_date,
    src_broadcast_network_name,
    src_program_name,
    src_market_break,
    src_daypart_name,
    src_playback_period_cd,
    src_demographic_group,
    mxm_source,
    program_telecast_rpt_starttime,
    program_telecast_rpt_endtime,
    min_of_pgm_value,
    most_current_audience_avg_pct,
    most_current_us_audience_avg_proj_000,
    most_current_nw_cvg_area_avg_pct,
    b.showdbid,
    b.title,
    b.subtitle,
    b.episodenumber,
    b.airdate,
    b.inpoint,
    b.outpoint,
    b.inpoint_24hr_est,
    b.modified_inpoint_1  AS modified_inpoint,
    b.modified_outpoint_1 AS modified_outpoint,
    b.segmenttype,
    b.comment,
    b.matchtype,
    b.talentactions,
    b.move,
    b.finishtype,
    b.recorddate,
    b.fileid,
    b.duration,
    b.additionaltalent,
    b.announcers,
    b.matchtitle,
    b.venuelocation,
    b.venuename,
    b.issegmentmarker,
    b.logentrydbid,
    b.logentryguid,
    b.loggername,
    b.logname,
    b.masterclipid,
    b.modifieddatetime,
    b.networkassetid,
    b.sponsors,
    b.weapon,
    b.season,
    b.source_ffed_name
FROM
    {{source('fds_nl','fact_nl_minxmin_ratings')}} a
LEFT JOIN
    {{ref('intm_nl_lite_log_est_modified')}} b
ON
    TRUNC(a.broadcast_date) = b.airdate
AND LOWER(trim(a.mxm_source)) = COALESCE(b.title_1, LOWER(trim(b.title)))
AND (
        DATEADD(MIN, (a.min_of_pgm_value - 1), (TRUNC(a.broadcast_date) || ' ' || trim
        (a.program_telecast_rpt_starttime))::TIMESTAMP)) >= b.modified_inpoint
AND (
        DATEADD(MIN, (a.min_of_pgm_value - 1), (TRUNC(a.broadcast_date) || ' ' || trim
        (a.program_telecast_rpt_starttime))::TIMESTAMP)) <= b.modified_outpoint
WHERE
    a.min_of_pgm_value IS NOT NULL
AND a.program_telecast_rpt_starttime IS NOT NULL
AND a.program_telecast_rpt_starttime <> ' '