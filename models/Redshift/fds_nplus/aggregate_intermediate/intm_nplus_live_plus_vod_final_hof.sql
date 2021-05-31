{{ config({ "schema": 'fds_nplus', "materialized": 'table',"tags": 'hof_live_vod_kickoff' }) }}
SELECT
        a.*                                                            ,
        (a.views*1.00)/a.prev_month_views-1                             AS monthly_per_change_views,
        (a.views*1.00)/a.prev_year_views-1                              AS yearly_per_change_views ,
        (EXTRACT(EPOCH FROM ((end_time) - (start_time)))/60::numeric)+1 AS duration                ,
        row_number() OVER (PARTITION BY
                           a.platform ORDER BY
                           a.views DESC)                                                                                                                                                                                                                        AS overall_rank                    ,
        CASE WHEN yearly_rank                          >0 THEN yearly_rank ELSE NULL END                                                                                                                                                                                           AS yearly_rank  ,
        CASE WHEN lower(a.event)                    LIKE '%wrestlemania%' THEN 'Tier 1' WHEN lower(a.event) LIKE '%royal rumble%' THEN 'Tier 1' WHEN lower(a.event) LIKE '%summerslam%' THEN 'Tier 1' WHEN lower(a.event) LIKE '%survivor series%' THEN 'Tier 1' ELSE 'Tier 2' END AS tier         ,
        CASE WHEN (a.views*1.00)/a.prev_month_views-1 >= 0 THEN '1' ELSE '0' END                                                                                                                                                                                                   AS monthly_color,
        CASE WHEN (a.views*1.00)/a.prev_year_views-1  >= 0 THEN '1' ELSE '0' END                                                                                                                                                                                                   AS yearly_color ,
        CASE WHEN a.event_date                         =
                        (
                                SELECT
                                        MAX(event_date)
                                FROM
                                        {{ ref("intm_nplus_live_plus_vod_consolidation_hof") }}) THEN 'Most Recent PPV' ELSE 'Prior PPVs' END AS Choose_PPV
FROM
        {{ ref("intm_nplus_live_plus_vod_consolidation_hof") }} a
LEFT JOIN
        (
                SELECT
                        platform  ,
                        event     ,
                        event_date,
                        views     ,
                        (row_number() OVER (PARTITION BY
                                            platform ORDER BY
                                            views DESC)) AS yearly_rank
                FROM
                        {{ ref("intm_nplus_live_plus_vod_consolidation_hof") }}) AS b
ON
        a.platform  =b.platform
AND     a.event     =b.event
AND     a.event_date=b.event_date