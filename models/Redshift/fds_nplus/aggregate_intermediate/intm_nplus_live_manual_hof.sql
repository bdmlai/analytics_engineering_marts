{{ config({ "schema": 'fds_nplus', "materialized": 'table',"tags": 'hof_live_kickoff' }) }}
SELECT
        report_name    ,
        event          ,
        event_name     ,
        event_brand    ,
        series_name    ,
        event_date     ,
        start_timestamp,
        end_timestamp  ,
        content_wwe_id ,
        production_id  ,
        platform       ,
        account        ,
        url            ,
        asset_id       ,
        data_level     ,
        views          ,
        minutes
FROM
        {{source('udl_nplus','raw_da_weekly_live_vod_kickoff_show_dashboard')}}
WHERE
        event_brand = 'Hall Of Fame'
AND     data_level  = 'Live'
AND     event_date  = TRUNC(convert_timezone('AMERICA/NEW_YORK', SYSDATE))
AND     as_on_date  = TRUNC(convert_timezone('AMERICA/NEW_YORK', SYSDATE))