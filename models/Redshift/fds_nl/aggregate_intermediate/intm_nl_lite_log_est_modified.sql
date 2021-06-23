{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    showdbid,
    title,
    CASE
        WHEN LOWER(trim(title)) LIKE '%nxt%'
        THEN 'nxt'
        ELSE NULL
    END AS title_1,
    subtitle,
    episodenumber,
    airdate,
    inpoint,
    outpoint,
    inpoint_24hr_est,
    modified_inpoint,
    modified_outpoint,
    ABS(DATEDIFF(sec, DATEADD(hr,12,(airdate || ' ' || substring(trim(inpoint), 1, 8))::
    TIMESTAMP), modified_inpoint)) AS in_diff,
    ABS(DATEDIFF(sec, DATEADD(hr,12,(airdate || ' ' || substring(trim(outpoint), 1, 8))::
    TIMESTAMP), modified_outpoint)) AS out_diff,
    CASE
        WHEN modified_inpoint=modified_outpoint
        AND in_diff < out_diff
        THEN DATEADD(MIN,1,modified_outpoint)
        ELSE modified_outpoint
    END AS modified_outpoint_1,
    CASE
        WHEN modified_inpoint=modified_outpoint
        AND in_diff > out_diff
        THEN DATEADD(MIN,-1,modified_inpoint)
        ELSE modified_inpoint
    END AS modified_inpoint_1,
    segmenttype,
    comment,
    matchtype,
    talentactions,
    move,
    finishtype,
    recorddate,
    fileid,
    duration,
    additionaltalent,
    announcers,
    matchtitle,
    venuelocation,
    venuename,
    issegmentmarker,
    logentrydbid,
    logentryguid,
    loggername,
    logname,
    masterclipid,
    modifieddatetime,
    networkassetid,
    sponsors,
    weapon,
    season,
    source_ffed_name
FROM
    {{ref('intm_nl_lite_log_est')}}