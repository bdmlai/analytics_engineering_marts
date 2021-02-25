{{
  config({
		'schema': 'fds_cpg',
		"materialized": 'view','tags': "Phase 5B"
  })
}}

SELECT
    dt.cal_year_week_begin_date AS week_date,
    cpg_session.property as property,
    cpg_session.channel_grouping as channel_grouping,
    cpg_session.trafficsource_source as trafficsource_source,
    SUM(cpg_session.sessions) AS Sessions,
    (SUM(cpg_session.transactions)::NUMERIC(20,2) / SUM(cpg_session.sessions))::NUMERIC(5,4) AS Conversion,
    (SUM(cpg_session.revenue) / SUM(cpg_session.transactions)::NUMERIC(20,2))::NUMERIC(10,4) AS AOV
FROM
    {{ref('aggr_cpg_daily_shops_sessions')}} cpg_session
JOIN
    {{source('cdm', 'dim_date')}} dt
ON
    (cpg_session.date = TRUNC(dt.full_date))
GROUP BY
    week_date,
    cpg_session.property,
    cpg_session.channel_grouping ,
    cpg_session.trafficsource_source
ORDER BY
    week_date,
    cpg_session.property,
    cpg_session.channel_grouping ,
    cpg_session.trafficsource_source