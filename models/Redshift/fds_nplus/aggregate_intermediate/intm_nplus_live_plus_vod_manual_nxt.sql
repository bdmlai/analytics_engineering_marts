{{ config({ "schema": 'fds_nplus', "materialized": 'table',"tags": 'nxt_live_vod_kickoff' }) }}
SELECT *
FROM
        {{source('udl_nplus','raw_da_weekly_live_vod_kickoff_show_dashboard')}}
WHERE
        event_brand IN ('NXT')
AND     data_level = 'Live+VOD'
AND     event_date = TRUNC(convert_timezone('AMERICA/NEW_YORK', SYSDATE))-1
AND     as_on_date = TRUNC(convert_timezone('AMERICA/NEW_YORK', SYSDATE))