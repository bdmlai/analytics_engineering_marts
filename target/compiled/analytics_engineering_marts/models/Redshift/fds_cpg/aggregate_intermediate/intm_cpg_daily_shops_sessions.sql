

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
    "entdwdb"."hive_udl_cpg"."ga_cpg_daily_sessions"
WHERE
    date >= '2021-01-19' and date <= '2021-01-31'