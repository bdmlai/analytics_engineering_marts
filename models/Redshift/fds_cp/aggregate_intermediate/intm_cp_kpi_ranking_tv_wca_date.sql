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
    --Added column series_type  as part of KPI ranking tv enhancement .
    -- Jira - PSTA-1897
    series_type,
    SUM(weekly_cumulative_audience) AS weekly_aud,
    SUM(telecasts_count)            AS telecasts_count
FROM
    {{source('fds_kntr','vw_aggr_kntr_schedule_wca_data')}}
WHERE
    LOWER(demographic_type)='everyone'
AND extract(YEAR FROM week_start_date)>extract(YEAR FROM DATEADD(YEAR,-3,GETDATE()))
AND week_start_date < substring(date_trunc('month',GETDATE()),1,10)
GROUP BY
    1,2,3,4,5