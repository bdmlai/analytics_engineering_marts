{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT DISTINCT
    c.showdbid,
    DATEDIFF(sec, (c.airdate || ' ' || substring(trim(c.min_inpoint), 1, 8))::TIMESTAMP,
    (d.air_date || ' ' || substring(trim(d.start_time_eastern), 1, 8))::TIMESTAMP) AS est_time_diff
FROM
    (
        SELECT DISTINCT
            a.showdbid,
            a.min_inpoint,
            b.airdate,
            b.logname
        FROM
            (
                SELECT
                    showdbid,
                    MIN(inpoint) AS min_inpoint
                FROM
                    {{source('udl_nplus','raw_lite_log')}}
                WHERE
                    (
                        LOWER(trim(title)) IN ('nxt',
                                               'raw',
                                               'smackdown')
                    OR  LOWER(trim(title)) LIKE '%nxt%')
                AND showdbid IS NOT NULL
                AND showdbid <> 0
                AND inpoint IS NOT NULL
                AND inpoint <> ' '
                AND segmenttype IS NOT NULL
                AND LOWER(trim(logname)) NOT IN
                    (
                        SELECT
                            logname
                        FROM
                            {{ref('intm_nl_excluded_logs')}})
                GROUP BY
                    1) a
        JOIN
            {{source('udl_nplus','raw_lite_log')}} b
        ON
            a.showdbid = b.showdbid
        AND a.min_inpoint = b.inpoint) c
JOIN
    {{source('udl_emm','emm_weekly_log_reference')}} d
ON
    c.airdate = d.air_date
AND LOWER(trim(c.logname)) = LOWER(trim(d.logname))
WHERE
    d.start_time_eastern IS NOT NULL
AND d.start_time_eastern NOT IN ('0',
                                 ' ')