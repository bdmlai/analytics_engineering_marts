{{ config({ "schema": 'fds_nplus', "materialized": 'ephemeral',"tags": 'bump_liveshow' }) }}
SELECT *
FROM
        (
                SELECT
                        event           ,
                        event_name      ,
                        event_brand     ,
                        report_name     ,
                        series_name     ,
                        event_date      ,
                        start_time      ,
                        end_time        ,
                        platform        ,
                        views           ,
                        minutes         ,
                        peak_concurrents,
                        prev_week_views ,
                        prev_week_event ,
                        production_id   ,
                        content_wwe_id  ,
                        data_level      ,
                        weekly_flag
                FROM
                        {{ ref("intm_nplus_live_manual_bump_base1") }}
                
                UNION ALL
                
                SELECT
                        event                                    ,
                        event_name                               ,
                        event_brand                              ,
                        report_name                              ,
                        series_name                              ,
                        event_date                               ,
                        start_time                               ,
                        end_time                                 ,
                        'Total'               AS platform        ,
                        SUM(views)            AS views           ,
                        SUM(minutes)          AS minutes         ,
                        SUM(peak_concurrents) AS peak_concurrents,
                        SUM(prev_week_views)  AS prev_week_views ,
                        prev_week_event                          ,
                        '' AS production_id                      ,
                        '' AS content_wwe_id                     ,
                        data_level                               ,
                        weekly_flag
                FROM
                        {{ ref("intm_nplus_live_manual_bump_base1") }}
                WHERE
                        platform <> 'Social'
                GROUP BY
                        event          ,
                        event_name     ,
                        event_brand    ,
                        report_name    ,
                        series_name    ,
                        event_date     ,
                        start_time     ,
                        end_time       ,
                        prev_week_event,
                        data_level     ,
                        weekly_flag
                
                UNION ALL
                
                SELECT
                        event                                    ,
                        event_name                               ,
                        event_brand                              ,
                        report_name                              ,
                        series_name                              ,
                        event_date                               ,
                        start_time                               ,
                        end_time                                 ,
                        'Social'              AS platform        ,
                        SUM(views)            AS views           ,
                        SUM(minutes)          AS minutes         ,
                        SUM(peak_concurrents) AS peak_concurrents,
                        SUM(prev_week_views)  AS prev_week_views ,
                        prev_week_event                          ,
                        '' AS production_id                      ,
                        '' AS content_wwe_id                     ,
                        data_level                               ,
                        weekly_flag
                FROM
                        {{ ref("intm_nplus_live_manual_bump_base1") }}
                WHERE
                        platform IN ('Facebook',
                                     'YouTube' ,
                                     'Twitter' ,
                                     'Twitch')
                GROUP BY
                        event          ,
                        event_name     ,
                        event_brand    ,
                        report_name    ,
                        series_name    ,
                        event_date     ,
                        start_time     ,
                        end_time       ,
                        prev_week_event,
                        data_level     ,
                        weekly_flag )