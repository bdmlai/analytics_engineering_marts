{{
  config({
		"materialized": 'table',
		"schema": 'dt_prod_support'
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
  total_adds,
  etl_insert_dtmm
FROM {{ref('intm_ppv_final_table')}}
WHERE event_type = 'current_ppv'
UNION all
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
  total_adds,
  etl_insert_dtmm
FROM {{ref('intm_ppv_final_table')}}
WHERE event_type != 'current_ppv'
  AND adds_day_of_week IN ('Friday', 'Saturday', 'Sunday')