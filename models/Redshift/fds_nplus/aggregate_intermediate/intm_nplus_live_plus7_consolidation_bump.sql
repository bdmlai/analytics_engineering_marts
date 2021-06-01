{{ config({ "schema": 'fds_nplus', "materialized": 'ephemeral',"tags": 'bump_liveshow' }) }}
SELECT *
FROM
        (
                SELECT
                        event                          ,
                        event_name                     ,
                        event_brand                    ,
                        report_name                    ,
                        series_name                    ,
                        event_date                     ,
                        start_time                     ,
                        end_time                       ,
                        platform                       ,
                        views                          ,
                        minutes                        ,
                        peak_concurrents               ,
                        prev_week_views                ,
                        prev_week_event                ,
                        production_id                  ,
                        content_wwe_id AS content_wweid,
                        data_level                     ,
                        weekly_flag
                FROM
                        {{ ref("intm_nplus_live_plus7_manual_bump_base_total") }}
                
                UNION ALL
                
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
                        content_wweid   ,
                        data_level      ,
                        weekly_flag
                FROM
                        {{source('fds_nplus','rpt_network_weekly_bump_live')}}
                WHERE
                        event_date <> TRUNC(convert_timezone('AMERICA/NEW_YORK', SYSDATE))-7
                AND     data_level  = 'Live+7' )