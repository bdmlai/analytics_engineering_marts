

SELECT
    date,
    property ,
    channel_grouping ,
    trafficsource_source,
    sessions,
    Transactions,
    Revenue,
    etl_batch_id,
    etl_insert_user_id,
    etl_insert_rec_dttm,
    etl_update_user_id,
    etl_update_rec_dttm
FROM
    "entdwdb"."fds_cpg"."aggr_cpg_daily_shops_sessions"