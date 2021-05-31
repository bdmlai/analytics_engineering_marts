{{ config({ "schema": 'fds_nplus', "materialized": 'ephemeral',"tags": 'bump_liveshow' }) }}
SELECT *
FROM
        {{source('udl_cp','live_daily_bump_shows')}}
WHERE
        event_brand IN ('WWE''s The Bump')
AND     data_level = 'Live'
AND     event_date = TRUNC(convert_timezone('AMERICA/NEW_YORK', SYSDATE))
AND     as_on_date = TRUNC(convert_timezone('AMERICA/NEW_YORK', SYSDATE))