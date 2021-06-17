{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT as_on_date,
    event_name,
    event_date,
    event_timestamp AS event_dttm,
    event_reporting_type,
    event_type,
    update_date
FROM {{source('udl_nplus', 'raw_da_weekly_ppv_hourly_comps_new')}}
WHERE update_date = (
        SELECT max(update_date)
        FROM {{source('udl_nplus', 'raw_da_weekly_ppv_hourly_comps_new')}}
    )
    AND as_on_date = (
        SELECT max(as_on_date)
        FROM {{source('udl_nplus', 'raw_da_weekly_ppv_hourly_comps_new')}}
    )
    AND EXISTS (
        SELECT 1
        FROM {{source('udl_nplus', 'raw_da_weekly_ppv_hourly_comps_new')}}
        WHERE event_date BETWEEN trunc(convert_timezone('AMERICA/NEW_YORK', sysdate -1)) AND trunc(convert_timezone('AMERICA/NEW_YORK', sysdate + 7))
            AND event_type = 'current_ppv'
    )