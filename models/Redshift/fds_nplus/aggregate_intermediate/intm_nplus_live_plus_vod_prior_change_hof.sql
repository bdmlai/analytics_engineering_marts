{{ config({ "schema": 'fds_nplus', "materialized": 'table',"tags": 'hof_live_vod_kickoff' }) }}
SELECT
        a.*            ,
        prev_year_views,
        prev_year_event
FROM
        (
                SELECT
                        platform                 ,
                        views AS prev_month_views,
                        event AS prev_month_event
                FROM
                        {{source('fds_nplus','rpt_nplus_yearly_hof_live_vod')}}
                WHERE
                        data_level  = 'Live+VOD'
                AND     event_brand = 'Hall Of Fame'
                AND     event_date IN
                        (
                                SELECT
                                        MAX(event_date)
                                FROM
                                        {{source('fds_nplus','rpt_nplus_yearly_hof_live_vod')}}
                                WHERE
                                        data_level  = 'Live+VOD'
                                AND     event_brand = 'Hall Of Fame'
                                AND     event_date <> TRUNC(convert_timezone('AMERICA/NEW_YORK', SYSDATE))-1 )) AS a
JOIN
        (
                SELECT
                        platform                     ,
                        AVG(views)                     AS prev_year_views,
                        'Avg. Prior Red Carpet Events' AS prev_year_event
                FROM
                        {{source('fds_nplus','rpt_nplus_yearly_hof_live_vod')}}
                WHERE
                        data_level  = 'Live+VOD'
                AND     event_brand = 'Hall Of Fame'
                AND     event_date  < TRUNC(convert_timezone('AMERICA/NEW_YORK', SYSDATE))-400
                GROUP BY
                        platform,
                        prev_year_event ) AS b
ON
        a.platform=b.platform