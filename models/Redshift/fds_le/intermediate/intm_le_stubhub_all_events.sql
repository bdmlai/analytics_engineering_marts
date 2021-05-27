{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    a.*,
    extract(year FROM trunc(a.eventdatelocal))         AS event_year,
    extract(month FROM trunc(a.eventdatelocal))        AS event_month,
    extract(day FROM trunc(a.eventdatelocal))          AS event_day,
    trunc(a.eventdatelocal)                            AS eventdatelocal_2,
    b.as_on_date_2,
    b.as_on_date_rank
FROM
    {{source('hive_udl_le','le_daily_stubhub_all_events')}} a
LEFT JOIN
    {{ref('intm_le_stubhub_as_on_date_rank')}} b
ON
    to_date(a.as_on_date, 'yyyymmdd') = b.as_on_date_2 