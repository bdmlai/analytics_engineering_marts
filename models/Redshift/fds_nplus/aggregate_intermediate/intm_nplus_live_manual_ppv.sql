{{ config({ "schema": 'fds_nplus', "materialized": 'table',"tags": 'ppv_live_kickoff' }) }}
SELECT *
FROM
        {{source('udl_nplus','raw_da_weekly_live_vod_kickoff_show_dashboard')}}
WHERE
        event_brand IN ('PPV')
AND     data_level = 'Live'
AND     event_date = TRUNC(convert_timezone('AMERICA/NEW_YORK', SYSDATE))
AND     as_on_date = TRUNC(convert_timezone('AMERICA/NEW_YORK', SYSDATE))