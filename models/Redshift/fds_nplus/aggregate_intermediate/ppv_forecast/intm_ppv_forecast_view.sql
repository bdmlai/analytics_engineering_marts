{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT bill_day_of_week,
  current_day_forecast,
  weekend_forecast,
  forecast_event_dt
FROM {{ref('intm_ppv_forecast')}},
  (
    SELECT sum(current_day_forecast) AS weekend_forecast
    FROM {{ref('intm_ppv_forecast')}}
  )
WHERE trunc(bill_date) = date(
    dateadd(
      'hour',
      -1,
      convert_timezone('AMERICA/NEW_YORK', getdate())
    )
  )