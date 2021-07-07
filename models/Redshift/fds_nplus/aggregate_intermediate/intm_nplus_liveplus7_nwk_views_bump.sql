{{ config({ "schema": 'fds_nplus', "materialized": 'ephemeral',"tags": 'bump_liveshow' }) }}
SELECT *
        ,
        'WWE''s The Bump' AS event_brand
FROM
        (
                SELECT
                        production_id               ,
                        content_title               ,
                        TRUNC(first_stream)        AS debut,
                        COUNT(DISTINCT src_fan_id) AS views,
                        SUM(play_time)          AS minutes
                FROM
                        {{source('fds_nplus','fact_daily_content_viewership')}}
                WHERE
                        lower(production_id) IN
                        (
                                SELECT
                                        lower(production_id)
                                FROM
                                        {{ ref("intm_nplus_live_plus7_manual_bump_base") }}
                                WHERE
                                        platform = 'Network')
                AND     stream_start_dttm < _7day_dt
                GROUP BY
                        1,
                        2,
                        3 )