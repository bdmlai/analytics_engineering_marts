{{
  config({
		'schema': 'fds_cpg',
		"materialized": 'ephemeral','tags': "Phase 5B"
  })
}}

SELECT
    date,
    property,
    visit_id_calc,
    total_visits,
    channel_grouping,
    trafficsource_source,
    total_transactions,
    total_totalTransactionRevenue
FROM
    {{source('hive_udl_cpg','ga_cpg_daily_sessions')}}
WHERE
    date >= '{{ var("start_date") }}' and date <= '{{ var("end_date") }}'
