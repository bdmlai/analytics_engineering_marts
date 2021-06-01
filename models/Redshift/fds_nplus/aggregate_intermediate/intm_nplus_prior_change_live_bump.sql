{{ config({ "schema": 'fds_nplus', "materialized": 'ephemeral',"tags": 'bump_liveshow' }) }}
SELECT *
FROM
        (
                SELECT
                        platform                ,
                        views AS prev_week_views,
                        event AS prev_week_event,
                        event_brand             ,
                        weekly_flag
                FROM
                        {{source('fds_nplus','rpt_nplus_weekly_bump_live')}}
                WHERE
                        data_level = 'Live'
                AND     event_brand IN
                        (
                                SELECT DISTINCT
                                        event_brand
                                FROM
                                        {{ ref("intm_nplus_live_manual_bump") }})
                AND     event IN
                        (
                                SELECT DISTINCT
                                        prev_week_event
                                FROM
                                        {{ ref("intm_nplus_live_manual_bump") }})
                AND     weekly_flag IN
                        (
                                SELECT DISTINCT
                                        weekly_flag
                                FROM
                                        {{ ref("intm_nplus_live_manual_bump") }})
                AND     platform NOT IN ('Total',
                                         'Social'))