{{ config({ "schema": 'fds_nplus', "materialized": 'table',"tags": 'hof_live_kickoff' }) }}
SELECT
        MAX(sum_max_value_plays) AS dotcom_plays,
        MIN(sum_max_value_plays) AS dotcom_us_plays
FROM
        (
                SELECT
                        filter_id  ,
                        filter_name,
                        SUM(max_value) AS sum_max_value_plays
                FROM
                        {{source('udl_nplus','raw_conviva_pulse_realtime')}}
                WHERE
                        account_name='WWE .Com'
                AND     filter_id IN ('138579',
                                      '138580')
                AND     min_time_est BETWEEN
                                             (
                                                     SELECT
                                                             start_timestamp
                                                     FROM
                                                             {{ ref("intm_nplus_live_manual_hof") }}
                                                     WHERE
                                                             platform = 'WWE.COM')
                AND
                        (
                                SELECT
                                        DATEADD(s,59,end_timestamp)
                                FROM
                                        {{ ref("intm_nplus_live_manual_hof") }}
                                WHERE
                                        platform = 'WWE.COM')
                GROUP BY
                        filter_id,
                        filter_name)