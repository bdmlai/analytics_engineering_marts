{{ config({ "schema": 'fds_nplus', "materialized": 'table',"tags": 'hof_live_vod_kickoff' }) }}
SELECT
        report_name                  ,
        event                        ,
        event_name                   ,
        event_brand                  ,
        series_name                  ,
        event_date                   ,
        start_timestamp AS start_time,
        end_timestamp   AS end_time  ,
        prev_month_event             ,
        prev_year_event              ,
        platform                     ,
        data_level                   ,
        content_wwe_id               ,
        production_id                ,
        account::varchar             ,
        url                          ,
        asset_id                     ,
        views                        ,
        minutes                      ,
        prev_month_views             ,
        prev_year_views              ,
        us_views                     ,
        CASE WHEN NVL(us_views,0) > 0 AND     NVL(views,0) > 0 THEN (us_views*1.00)/views ELSE NULL END AS per_us_views
FROM
        {{ ref("intm_nplus_live_plus_vod_manual_base_total_hof") }}

UNION ALL

SELECT
        report_name      ,
        event            ,
        event_name       ,
        event_brand      ,
        series_name      ,
        event_date       ,
        start_time       ,
        end_time         ,
        prev_month_event ,
        prev_year_event  ,
        platform         ,
        data_level       ,
        content_wweid    ,
        production_id    ,
        account          ,
        url              ,
        asset_id::varchar,
        views            ,
        minutes          ,
        prev_month_views ,
        prev_year_views  ,
        us_views         ,
        per_us_views
FROM
        {{source('fds_nplus','rpt_nplus_yearly_hof_live_vod')}}
WHERE
        event_brand = 'Hall Of Fame'
AND     data_level  = 'Live+VOD'
AND     event_date <> TRUNC(convert_timezone('AMERICA/NEW_YORK', SYSDATE))-1