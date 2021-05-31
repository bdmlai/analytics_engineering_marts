{{ config({ "schema": 'fds_nplus', "materialized": 'table',"tags": 'ppv_live_kickoff' }) }}
SELECT
        COUNT(DISTINCT a.customerid) AS unique_viewers,
        'PPV'                        AS event_brand
FROM
        (
                SELECT
                        b.*,
                        CASE WHEN max_time >=
                                        (
                                                SELECT
                                                        DATEADD(m,6,start_timestamp)
                                                FROM
                                                        {{ ref("intm_nplus_live_manual_ppv") }}
                                                WHERE
                                                        platform = 'Network' AND     event_brand = 'PPV') --  (START TIME + 6 MINUTES, EST)
                                AND     min_time <
                                        (
                                                SELECT
                                                        DATEADD(m,-4,end_timestamp)
                                                FROM
                                                        {{ ref("intm_nplus_live_manual_ppv") }}
                                                WHERE
                                                        platform = 'Network' AND     event_brand = 'PPV') -- (END TIME - 5 MINUTES, EST)
                                THEN 1 ELSE 0 END AS ppv_flag
                FROM
                        (
                                SELECT
                                        c.*,
                                        (EXTRACT(EPOCH FROM (max_time-min_time))/60::numeric) AS time_spent
                                FROM
                                        {{ ref("intm_nplus_live_nwk_unique_viewers_t1_ppv") }} c ) b
                WHERE
                        b.time_spent>=6 ) a
WHERE
        a.ppv_flag='1'