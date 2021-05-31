{{ config({ "schema": 'fds_nplus', "materialized": 'table',"tags": 'nxt_live_kickoff' }) }}
SELECT
        COUNT(DISTINCT a.customerid) AS unique_viewers,
        'NXT'                        AS event_brand
FROM
        (
                SELECT
                        b.*,
                        CASE WHEN max_time >=
                                        (
                                                SELECT
                                                        DATEADD(m,6,start_timestamp)
                                                FROM
                                                        {{ ref("intm_nplus_live_manual_nxt") }}
                                                WHERE
                                                        platform = 'Network' AND     event_brand = 'NXT') --  (START TIME + 6 MINUTES, EST)
                                AND     min_time <
                                        (
                                                SELECT
                                                        DATEADD(m,-4,end_timestamp)
                                                FROM
                                                        {{ ref("intm_nplus_live_manual_nxt") }}
                                                WHERE
                                                        platform = 'Network' AND     event_brand = 'NXT') -- (END TIME - 5 MINUTES, EST)
                                THEN 1 ELSE 0 END AS nxt_flag
                FROM
                        (
                                SELECT
                                        c.*,
                                        (EXTRACT(EPOCH FROM (max_time-min_time))/60::numeric) AS time_spent
                                FROM
                                        {{ ref("intm_nplus_live_nwk_unique_viewers_t1_nxt") }} c ) b
                WHERE
                        b.time_spent>=6 ) a
WHERE
        a.nxt_flag='1'