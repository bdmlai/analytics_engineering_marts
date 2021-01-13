{{
  config({
		'schema': 'fds_cpg',
		"materialized": 'view','tags': "Phase 5B"
  })
}}

SELECT
    TRUNC(date_trunc('month', cpg_session.date)) AS month_date,
    cpg_session.property as property,
    cpg_session.channel as Channel,
    SUM(cpg_session.sessions) AS Sessions,
    (SUM(cpg_session.transactions)::NUMERIC(20,2) / SUM(cpg_session.sessions))::NUMERIC(5,4) AS Conversion,
    (SUM(cpg_session.revenue) / SUM(cpg_session.transactions)::NUMERIC(20,2))::NUMERIC(10,4) AS AOV
FROM
    {{ref('aggr_cpg_daily_shops_sessions')}} cpg_session
GROUP BY
    month_date,
    cpg_session.property,
    cpg_session.channel
ORDER BY
    month_date,
    cpg_session.property,
    cpg_session.channel