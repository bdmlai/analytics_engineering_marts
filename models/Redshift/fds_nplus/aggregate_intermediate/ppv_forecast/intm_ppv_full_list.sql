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
  0 AS adds_days_to_event,
  event_date AS adds_date
FROM {{ref('intm_ppv_current_ppv')}}
UNION all
SELECT event_date,
  event_dttm,
  event_name,
  event_type,
  event_reporting_type,
  -1 AS adds_days_to_event,
  event_date -1 AS adds_date
FROM {{ref('intm_ppv_current_ppv')}}
UNION all
SELECT event_date,
  event_dttm,
  event_name,
  event_type,
  event_reporting_type,
  -2 AS adds_days_to_event,
  event_date -2 AS adds_date
FROM {{ref('intm_ppv_current_ppv')}}
UNION all
SELECT event_date,
  event_dttm,
  event_name,
  event_type,
  event_reporting_type,
  -3 AS adds_days_to_event,
  event_date -3 AS adds_date
FROM {{ref('intm_ppv_current_ppv')}}
UNION all
SELECT event_date,
  event_dttm,
  event_name,
  event_type,
  event_reporting_type,
  -4 AS adds_days_to_event,
  event_date -4 AS adds_date
FROM {{ref('intm_ppv_current_ppv')}}
UNION all
SELECT event_date,
  event_dttm,
  event_name,
  event_type,
  event_reporting_type,
  -5 AS adds_days_to_event,
  event_date -5 AS adds_date
FROM {{ref('intm_ppv_current_ppv')}}
UNION all
SELECT event_date,
  event_dttm,
  event_name,
  event_type,
  event_reporting_type,
  -6 AS adds_days_to_event,
  event_date -6 AS adds_date
FROM {{ref('intm_ppv_current_ppv')}}