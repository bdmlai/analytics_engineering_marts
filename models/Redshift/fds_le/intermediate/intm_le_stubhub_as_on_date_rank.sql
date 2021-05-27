{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    *,
    rank()over(ORDER BY as_on_date_2 DESC) AS as_on_date_rank
FROM
    (
        SELECT DISTINCT
            to_date(as_on_date, 'yyyymmdd') AS as_on_date_2
        FROM
            {{source('hive_udl_le','le_daily_stubhub_all_events')}})