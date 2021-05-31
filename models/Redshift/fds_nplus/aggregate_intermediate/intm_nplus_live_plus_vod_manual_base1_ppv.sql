{{ config({ "schema": 'fds_nplus', "materialized": 'table',"tags": 'ppv_live_vod_kickoff' }) }}
SELECT
        report_name     ,
        event           ,
        event_name      ,
        event_brand     ,
        series_name     ,
        event_date      ,
        start_timestamp ,
        end_timestamp   ,
        prev_month_event,
        prev_year_event ,
        platform        ,
        data_level      ,
        content_wwe_id  ,
        production_id   ,
        account         ,
        url             ,
        asset_id        ,
        CASE WHEN platform = 'Network' AND     event_brand = 'PPV' THEN
                        (
                                SELECT
                                        views
                                FROM
                                        {{ ref("intm_nplus_live_plus_vod_nwk_views_ppv") }}
                                WHERE
                                        country = 'GLOBAL' AND     event_brand = 'PPV') WHEN platform = 'WWE.COM' AND     event_brand = 'PPV' THEN
                        (
                                SELECT
                                        views
                                FROM
                                        {{source('fds_nplus','rpt_nplus_monthly_ppv_live')}}
                                WHERE
                                        event_brand = 'PPV' AND     data_level = 'Live' AND     event_date = TRUNC(convert_timezone('AMERICA/NEW_YORK', SYSDATE))-1 AND     platform='WWE.COM') WHEN platform = 'Twitch' AND     event_brand = 'PPV' THEN
                        (
                                SELECT
                                        views
                                FROM
                                        {{source('fds_nplus','rpt_nplus_monthly_ppv_live')}}
                                WHERE
                                        event_brand = 'PPV' AND     data_level = 'Live' AND     event_date = TRUNC(convert_timezone('AMERICA/NEW_YORK', SYSDATE))-1 AND     platform='Twitch') ELSE views END AS views,
        CASE WHEN platform = 'Network' AND     event_brand = 'PPV' THEN
                        (
                                SELECT
                                        minutes
                                FROM
                                        {{ ref("intm_nplus_live_plus_vod_nwk_views_ppv") }}
                                WHERE
                                        country = 'GLOBAL' AND     event_brand = 'PPV') WHEN platform = 'Twitch' AND     event_brand = 'PPV' THEN
                        (
                                SELECT
                                        minutes
                                FROM
                                        {{source('fds_nplus','rpt_nplus_monthly_ppv_live')}}
                                WHERE
                                        event_brand = 'PPV' AND     data_level = 'Live' AND     event_date = TRUNC(convert_timezone('AMERICA/NEW_YORK', SYSDATE))-1 AND     platform='Twitch') ELSE minutes END AS minutes,
        prev_month_views                                                                                                                                                                                                  ,
        prev_year_views                                                                                                                                                                                                   ,
        CASE WHEN platform = 'Network' AND     event_brand = 'PPV' THEN
                        (
                                SELECT
                                        views
                                FROM
                                        {{ ref("intm_nplus_live_plus_vod_nwk_views_ppv") }}
                                WHERE
                                        country = 'US' AND     event_brand = 'PPV') WHEN platform = 'WWE.COM' AND     event_brand = 'PPV' THEN
                        (
                                SELECT
                                        us_views
                                FROM
                                        {{source('fds_nplus','rpt_nplus_monthly_ppv_live')}}
                                WHERE
                                        event_brand = 'PPV' AND     data_level = 'Live' AND     event_date = TRUNC(convert_timezone('AMERICA/NEW_YORK', SYSDATE))-1 AND     platform='WWE.COM') ELSE us_views END AS us_views
FROM
        {{ ref("intm_nplus_live_plus_vod_manual_base_ppv") }}