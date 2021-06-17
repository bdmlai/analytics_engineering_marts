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
  convert_timezone('AMERICA/NEW_YORK', sysdate) AS etl_insert_dtmm
FROM {{ref('intm_ppv_final_table_up')}}