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
        account::varchar,
        url             ,
        asset_id        ,
        views           ,
        minutes         ,
        prev_month_views,
        prev_year_views ,
        us_views
FROM
        {{ ref("intm_nplus_live_plus_vod_manual_base1_ppv") }}

UNION ALL

SELECT
        report_name                              ,
        event                                    ,
        event_name                               ,
        event_brand                              ,
        series_name                              ,
        event_date                               ,
        start_timestamp                          ,
        end_timestamp                            ,
        prev_month_event                         ,
        prev_year_event                          ,
        'Total' AS platform                      ,
        data_level                               ,
        ''                    AS content_wwe_id  ,
        ''                    AS production_id   ,
        ''                    AS account         ,
        ''                    AS url             ,
        ''                    AS asset_id        ,
        SUM(views)            AS views           ,
        SUM(minutes)          AS minutes         ,
        SUM(prev_month_views) AS prev_month_views,
        SUM(prev_year_views)  AS prev_year_views ,
        SUM(us_views)         AS us_views
FROM
        {{ ref("intm_nplus_live_plus_vod_manual_base1_ppv") }}
WHERE
        platform <> 'Social'
GROUP BY
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
        data_level

UNION ALL

SELECT
        report_name                              ,
        event                                    ,
        event_name                               ,
        event_brand                              ,
        series_name                              ,
        event_date                               ,
        start_timestamp                          ,
        end_timestamp                            ,
        prev_month_event                         ,
        prev_year_event                          ,
        'Social' AS platform                     ,
        data_level                               ,
        ''                    AS content_wwe_id  ,
        ''                    AS production_id   ,
        ''                    AS account         ,
        ''                    AS url             ,
        ''                    AS asset_id        ,
        SUM(views)            AS views           ,
        SUM(minutes)          AS minutes         ,
        SUM(prev_month_views) AS prev_month_views,
        SUM(prev_year_views)  AS prev_year_views ,
        SUM(us_views)         AS us_views
FROM
        {{ ref("intm_nplus_live_plus_vod_manual_base1_ppv") }}
WHERE
        platform IN ('Facebook',
                     'YouTube' ,
                     'Twitter' ,
                     'Twitch')
GROUP BY
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
        data_level