{{ config({ "schema": 'fds_nplus', "materialized": 'ephemeral',"tags": 'bump_liveshow' }) }}
SELECT
        event                                                              ,
        event_name                                                         ,
        event_brand                                                        ,
        report_name                                                        ,
        series_name                                                        ,
        event_date                                                         ,
        start_time                                                         ,
        end_time                                                           ,
        platform                                                           ,
        views                                                              ,
        minutes                                                            ,
        peak_concurrents                                                   ,
        prev_week_views                                                    ,
        prev_week_event                                                    ,
        (views*1.00)/NULLIF(prev_week_views,0)-1                             AS weekly_per_change_views,
        ROUND((EXTRACT(EPOCH FROM ((end_time) - (start_time)))/60::numeric)) AS duration               ,
        row_number() OVER (PARTITION BY
                           platform ORDER BY
                           views DESC)                                                AS overall_rank,
        CASE WHEN (views*1.00)/NULLIF(prev_week_views,0)-1 >= 0 THEN '1' ELSE '0' END AS weekly_color,
        CASE WHEN event_date                                =
                        (
                                SELECT
                                        MAX(event_date)
                                FROM
                                        {{ ref("intm_nplus_live_consolidation_bump") }}) THEN 'Most Recent Event' ELSE 'Prior Event' END AS choose_event,
        production_id                                                                                                                                   ,
        content_wweid                                                                                                                                   ,
        data_level                                                                                                                                      ,
        weekly_flag
FROM
        {{ ref("intm_nplus_live_consolidation_bump") }}