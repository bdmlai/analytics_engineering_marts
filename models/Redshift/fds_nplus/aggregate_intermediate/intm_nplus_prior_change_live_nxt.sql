{{ config({ "schema": 'fds_nplus', "materialized": 'table',"tags": 'nxt_live_kickoff' }) }}
SELECT
        a.*            ,
        prev_year_views,
        prev_year_event
FROM
        (
                SELECT
                        platform                 ,
                        views AS prev_month_views,
                        event AS prev_month_event,
                        event_brand
                FROM
                        {{source('fds_nplus','rpt_nplus_monthly_nxt_live')}}
                WHERE
                        data_level = 'Live'
                AND     event_brand IN
                        (
                                SELECT DISTINCT
                                        event_brand
                                FROM
                                        {{ ref("intm_nplus_live_manual_nxt") }})
                AND     event IN
                        (
                                SELECT DISTINCT
                                        prev_month_event
                                FROM
                                        {{ ref("intm_nplus_live_manual_nxt") }})) AS a
JOIN
        (
                SELECT
                        platform                ,
                        views AS prev_year_views,
                        event AS prev_year_event,
                        event_brand
                FROM
                        {{source('fds_nplus','rpt_nplus_monthly_nxt_live')}}
                WHERE
                        data_level = 'Live'
                AND     event_brand IN
                        (
                                SELECT DISTINCT
                                        event_brand
                                FROM
                                        {{ ref("intm_nplus_live_manual_nxt") }})
                AND     event IN
                        (
                                SELECT DISTINCT
                                        prev_year_event
                                FROM
                                        {{ ref("intm_nplus_live_manual_nxt") }})) AS b
ON
        a.platform    =b.platform
AND     a.event_brand = b.event_brand
WHERE
        a.platform NOT IN ('Total',
                           'Social')