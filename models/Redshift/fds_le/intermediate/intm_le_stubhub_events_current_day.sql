{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    curr_day.*,
    0 AS event_dropped_flag,
    CASE
        WHEN prev_day.id IS NULL
        THEN 1
        ELSE 0
    END AS event_added_flag,
    CASE
        WHEN trunc(curr_day.lastupdateddate)>=CURRENT_DATE-15
        THEN 1
        ELSE 0
    END AS last_updated_within_15_days_flag,
    CASE
        WHEN curr_day.ticketinfo_totallistings=prev_day.ticketinfo_totallistings
        THEN 0
        ELSE 1
    END AS total_listings_changed_flag,
    CASE
        WHEN curr_day.lastupdateddate=prev_day.lastupdateddate
        THEN 0
        ELSE 1
    END AS last_updated_date_changed_flag,
    CASE
        WHEN LOWER(curr_day.name) LIKE '%parking%'
        THEN 1
        ELSE 0
    END AS event_created_for_parking_flag
FROM
    (
        SELECT
            *
        FROM
            {{ref('intm_le_stubhub_latest_as_on_date_events')}}
        WHERE
            as_on_date_rank=1) curr_day
LEFT JOIN
    (
        SELECT
            *
        FROM
            {{ref('intm_le_stubhub_latest_as_on_date_events')}}
        WHERE
            as_on_date_rank=2) prev_day
ON
    curr_day.id=prev_day.id