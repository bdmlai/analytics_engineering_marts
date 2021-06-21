{{ config({ "schema": 'fds_nplus', "materialized": 'table',"tags": 'ppv_live_kickoff' }) }}
SELECT
        a.*                                                            ,
        (a.views*1.00)/nullif(a.prev_month_views,0)-1                             AS monthly_per_change_views,
        (a.views*1.00)/nullif(a.prev_year_views,0)-1                              AS yearly_per_change_views ,
        (EXTRACT(EPOCH FROM ((end_time) - (start_time)))/60::numeric)+1 AS duration                ,
        row_number() OVER (PARTITION BY
                           a.platform ORDER BY
                           a.views DESC)                                                                                                                                                                                                                                                                     AS overall_rank                    ,
        CASE WHEN a.event_brand                        = 'PPV' AND     ppv_yearly_rank>0 THEN ppv_yearly_rank ELSE NULL END                                                                                                                                                                                                     AS yearly_rank  ,
        CASE WHEN lower(a.event)                    LIKE '%wrestlemania%' THEN 'Tier 1' WHEN lower(a.event) LIKE '%royal rumble%' AND     lower(a.event) NOT LIKE '%greatest%' THEN 'Tier 1' WHEN lower(a.event) LIKE '%summerslam%' THEN 'Tier 1' WHEN lower(a.event) LIKE '%survivor series%' THEN 'Tier 1' ELSE 'Tier 2' END AS tier         ,
        CASE WHEN (a.views*1.00)/nullif(a.prev_month_views,0)-1 >= 0 THEN '1' ELSE '0' END                                                                                                                                                                                                                                                AS monthly_color,
        CASE WHEN (a.views*1.00)/nullif(a.prev_year_views,0)-1  >= 0 THEN '1' ELSE '0' END                                                                                                                                                                                                                                                AS yearly_color ,
        CASE WHEN a.event_date                         =
                        (
                                SELECT
                                        MAX(event_date)
                                FROM
                                        {{ ref("intm_nplus_live_consolidation_ppv") }}) THEN 'Most Recent PPV' ELSE 'Prior PPVs' END AS Choose_PPV
FROM
        {{ ref("intm_nplus_live_consolidation_ppv") }} a
LEFT JOIN
        (
                SELECT
                        platform  ,
                        event     ,
                        event_date,
                        views     ,
                        (row_number() OVER (PARTITION BY
                                            platform ORDER BY
                                            views DESC)) AS ppv_yearly_rank
                FROM
                        {{ ref("intm_nplus_live_consolidation_ppv") }}
                WHERE
                        TRUNC(convert_timezone('AMERICA/NEW_YORK', SYSDATE))-event_date::date <=380
                AND     event_brand                                                            = 'PPV') AS b
ON
        a.platform  =b.platform
AND     a.event     =b.event
AND     a.event_date=b.event_date