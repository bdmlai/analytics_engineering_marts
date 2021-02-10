{{
  config({
		'schema': 'fds_cpg',
		"pre-hook": ["delete from fds_cpg.aggr_cpg_daily_shops_sessions where date >= '{{ var(\"start_date\") }}' and  date <= '{{ var(\"end_date\") }}'"],
		"materialized": 'incremental','tags': "Phase 5B"
  })
}}

SELECT
    date,
    property ,
    channel_grouping ,
    trafficsource_source ,
    COUNT(DISTINCT CONCAT(visit_id_calc, total_visits)) AS sessions,
    SUM(total_transactions) AS Transactions,
    SUM(total_totalTransactionRevenue)/1000000.0 AS Revenue,
    CAST(TO_CHAR(SYSDATE,'YYYYMMDDHHMISS') as bigint) as etl_batch_id,
    'etluser_cpg' as etl_insert_user_id,
    sysdate as etl_insert_rec_dttm,
    null as etl_update_user_id,
    cast(null as timestamp) as etl_update_rec_dttm
FROM
    {{ ref('intm_cpg_daily_shops_sessions') }}
GROUP BY
    date ,
    property,
    channel_grouping ,
    trafficsource_source
ORDER BY
    date,
    property,
    channel_grouping ,
    trafficsource_source