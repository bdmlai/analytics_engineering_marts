{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    a.showdbid,
    title,
    subtitle,
    episodenumber,
    airdate,
    inpoint,
    outpoint,
    (DATEADD(hr, 12, (DATEADD(sec, b.est_time_diff, (airdate || ' ' || substring(trim(inpoint), 1,
    8))::TIMESTAMP))))                                                          AS inpoint_24hr_est,
    ((substring((DATEADD(sec, 30, inpoint_24hr_est)), 1, 17) || '00')::TIMESTAMP) AS
    modified_inpoint,
    ((substring((DATEADD(sec, (((substring(duration, 1, 2))::INT * 60 * 60) + ((substring(duration,
    4, 2))::INT * 60) + ((substring(duration, 7, 2))::INT) + 30), inpoint_24hr_est)), 1, 17) ||
    '00')::TIMESTAMP) AS modified_outpoint,
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
    {{source('udl_nplus','raw_lite_log')}} a
JOIN
    {{ref('intm_nl_est_time_diff')}} b
ON
    a.showdbid = b.showdbid
WHERE
    airdate IS NOT NULL
AND inpoint IS NOT NULL
AND duration IS NOT NULL
AND inpoint <> ' '
AND duration <> ' '
AND LOWER(trim(logname)) NOT IN
    (
        SELECT
            logname
        FROM
            {{ref('intm_nl_excluded_logs')}})