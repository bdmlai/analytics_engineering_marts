{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT event_date,
  event_dttm,
  event_name,
  event_type,
  event_reporting_type,
  adds_days_to_event,
  adds_day_of_week,
  adds_date,
  adds_time,
  paid_adds,
  trial_adds,
  total_adds
FROM (
    SELECT a.event_date,
      a.event_dttm,
      a.event_name,
      a.event_type,
      a.event_reporting_type,
      a.adds_days_to_event,
      CASE
        WHEN date_part(dayofweek, a.adds_date) = 0 THEN 'Sunday'
        WHEN date_part(dayofweek, a.adds_date) = 1 THEN 'Monday'
        WHEN date_part(dayofweek, a.adds_date) = 2 THEN 'Tuesday'
        WHEN date_part(dayofweek, a.adds_date) = 3 THEN 'Wednesday'
        WHEN date_part(dayofweek, a.adds_date) = 4 THEN 'Thursday'
        WHEN date_part(dayofweek, a.adds_date) = 5 THEN 'Friday'
        WHEN date_part(dayofweek, a.adds_date) = 6 THEN 'Saturday'
        ELSE 'Other'
      END AS adds_day_of_week,
      a.adds_date,
      b.adds_time,
      b.paid_adds,
      b.trial_adds,
      b.total_adds
    FROM {{ref('intm_ppv_full_list')}} AS a
      LEFT JOIN (
        SELECT adds_date,
          adds_time,
          paid_adds,
          trial_adds,
          total_adds
        FROM {{ref('intm_ppv_hist_orders')}}
      ) AS b ON a.adds_date = b.adds_date
  )