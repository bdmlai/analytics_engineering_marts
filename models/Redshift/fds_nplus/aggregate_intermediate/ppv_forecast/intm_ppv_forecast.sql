{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT trunc(bill_date) AS bill_date,
    forecast_event_dt,
    CASE
        WHEN date_part(dayofweek, bill_date) = 0 THEN 'Sunday'
        WHEN date_part(dayofweek, bill_date) = 1 THEN 'Monday'
        WHEN date_part(dayofweek, bill_date) = 2 THEN 'Tuesday'
        WHEN date_part(dayofweek, bill_date) = 3 THEN 'Wednesday'
        WHEN date_part(dayofweek, bill_date) = 4 THEN 'Thursday'
        WHEN date_part(dayofweek, bill_date) = 5 THEN 'Friday'
        WHEN date_part(dayofweek, bill_date) = 6 THEN 'Saturday'
        ELSE 'Other'
    END AS bill_day_of_week,
    --sum(paid_new_adds+paid_winbacks+trial_adds) AS current_day_forecast --TAB-2028 sum(paid_new_adds+paid_winbacks) AS current_day_forecast
FROM {{source('fds_nplus', 'aggr_nplus_daily_forcast_output')}} a,
    (
        SELECT top 1 event_date AS forecast_event_dt,
            dateadd(day, -2, event_date) AS forecast_start_dt
        FROM {{source('udl_nplus', 'raw_da_weekly_ppv_hourly_comps_new')}}
        WHERE event_type = 'current_ppv'
            AND as_on_date = (
                SELECT max(as_on_date)
                FROM {{source('udl_nplus', 'raw_da_weekly_ppv_hourly_comps_new')}}
            )
            AND update_date = (
                SELECT max(update_date)
                FROM {{source('udl_nplus', 'raw_da_weekly_ppv_hourly_comps_new')}}
            )
    ) b
WHERE forecast_date = (
        SELECT max(forecast_date)
        FROM {{source('fds_nplus', 'aggr_nplus_daily_forcast_output')}}
        WHERE country_region NOT IN ('united states')
    )
    AND UPPER(payment_method) IN ('MLBAM', 'ROKU')
    AND Upper(official_run_flag) = 'OFFICIAL' --'ROKU''APPLE'
    AND trunc(bill_date) >= b.forecast_start_dt
    AND trunc(bill_date) <= b.forecast_event_dt
    AND country_region NOT IN ('united states')
GROUP BY bill_date,
    forecast_event_dt