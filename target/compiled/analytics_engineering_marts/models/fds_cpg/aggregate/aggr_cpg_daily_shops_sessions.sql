

with __dbt__CTE__intm_cpg_daily_shops_sessions as (


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
)SELECT
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
    __dbt__CTE__intm_cpg_daily_shops_sessions
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