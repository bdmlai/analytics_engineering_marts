{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT a.event_date,
  a.event_dttm,
  a.event_name,
  a.event_type,
  a.event_reporting_type,
  a.adds_days_to_event,
  a.adds_day_of_week,
  a.adds_date,
  a.adds_time_to_event,
  a.adds_time,
  a.paid_adds,
  a.trial_adds,
  a.total_adds,
  b.event_reporting_type AS current_event_reporting_type,
  b.event_name AS current_event_name,
  b.event_date AS current_event_date,
  b.event_dttm AS current_event_dttm,
  b.current_adds_date,
  b.current_adds_days_to_event,
  b.current_adds_day_of_week,
  b.current_adds_time,
  b.ghw_adds_tillnow,
  b.ghw_adds_estimate,
  b.currentday_adds_tillnow,
  b.currentday_adds_estimate,
  b.weekend_adds_tillnow,
  b.weekend_adds_estimate
FROM {{ref('intm_ppv_final_view')}} AS a
  LEFT JOIN {{ref('intm_ppv_estimates')}} AS b ON a.adds_days_to_event = b.current_adds_days_to_event