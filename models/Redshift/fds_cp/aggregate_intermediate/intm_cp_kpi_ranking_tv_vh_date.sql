{{
  config({
		
		"materialized": 'ephemeral'
  })
}}


SELECT
    week_start_date                     AS week,
    date_trunc('month',week_start_date) AS MONTH,
    initcap(src_country)                AS country,
    program_1                           AS program,
    --Added column series_type and telecast_hours as part of KPI ranking tv
    -- enhancement . Jira - PSTA-1897
    series_type,
    AVG(duration_mins)                        AS duration_mins,
    SUM(aud)                                  AS aud,
    ROUND(SUM(duration_mins/60 :: DECIMAL),2) AS telecast_hours
FROM
    {{source('fds_kntr','rpt_kntr_schedule_vh_data')}}
WHERE
    LOWER(demographic_type)='everyone'
AND extract(YEAR FROM week_start_date)>extract(YEAR FROM DATEADD(YEAR,-3,GETDATE()))
AND week_start_date < substring(date_trunc('month',GETDATE()),1,10)
GROUP BY
    1,2,3,4,5