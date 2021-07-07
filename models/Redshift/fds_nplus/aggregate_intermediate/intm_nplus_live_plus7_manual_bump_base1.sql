{{ config({ "schema": 'fds_nplus', "materialized": 'ephemeral',"tags": 'bump_liveshow' }) }}
SELECT
        event         ,
        event_name    ,
        event_brand   ,
        report_name   ,
        series_name   ,
        event_date    ,
        start_time,
        end_time  ,
        platform      ,
        CASE WHEN platform = 'Network' THEN
                        (
                                SELECT
                                        views
                                FROM
                                        {{ ref("intm_nplus_liveplus7_nwk_views_bump") }} ) ELSE views END AS views,
        CASE WHEN platform = 'Network' THEN
                        (
                                SELECT
                                        minutes
                                FROM
                                        {{ ref("intm_nplus_liveplus7_nwk_views_bump") }} ) ELSE minutes END AS minutes,
        CASE WHEN platform = 'Facebook' THEN
                        (
                                SELECT
                                        peak_concurrents
                                FROM
                                        {{source('fds_nplus','rpt_nplus_weekly_bump_live')}}
                                WHERE
                                        data_level = 'Live' AND     event_date = (select distinct event_date from {{ ref("intm_nplus_live_plus7_manual_bump") }}) AND     platform = 'Facebook') WHEN platform = 'YouTube' THEN
                        (
                                SELECT
                                        peak_concurrents
                                FROM
                                        {{source('fds_nplus','rpt_nplus_weekly_bump_live')}}
                                WHERE
                                        data_level = 'Live' AND     event_date = (select distinct event_date from {{ ref("intm_nplus_live_plus7_manual_bump") }}) AND     platform = 'YouTube') ELSE peak_concurrents END AS peak_concurrents,
        prev_week_views                                                                                                                                                                                            ,
        prev_week_event                                                                                                                                                                                            ,
        production_id                                                                                                                                                                                              ,
        content_wwe_id                                                                                                                                                                                             ,
        data_level                                                                                                                                                                                                 ,
        weekly_flag
FROM
        {{ ref("intm_nplus_live_plus7_manual_bump_base") }}