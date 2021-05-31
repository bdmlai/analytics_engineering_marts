{{ config({ "schema": 'fds_nplus', "materialized": 'table',"tags": 'ppv_live_vod_kickoff' }) }}
SELECT *
        ,
        'PPV' AS event_brand
FROM
        (
        (
                SELECT
                        'GLOBAL'                   AS country, ------- Network Views All
                        COUNT(DISTINCT src_fan_id) AS views  ,
                        SUM(play_time)             AS minutes
                FROM
                        {{source('fds_nplus','fact_daily_content_viewership')}}
                WHERE
                        lower(production_id) IN
                        (
                                SELECT
                                        lower(production_id)
                                FROM
                                        {{ ref("intm_nplus_live_plus_vod_manual_base_ppv") }}
                                WHERE
                                        platform    = 'Network'
                                AND     event_brand = 'PPV')
                AND     STREAM_START_DTTM BETWEEN first_stream AND     _same_day)

UNION ALL
        (
                SELECT
                        'US'                       AS country, ------- Network Views US
                        COUNT(DISTINCT src_fan_id) AS views  ,
                        SUM(play_time)             AS minutes
                FROM
                        {{source('fds_nplus','fact_daily_content_viewership')}}
                WHERE
                        lower(production_id) IN
                        (
                                SELECT
                                        lower(production_id)
                                FROM
                                        {{ ref("intm_nplus_live_plus_vod_manual_base_ppv") }}
                                WHERE
                                        platform    = 'Network'
                                AND     event_brand = 'PPV')
                AND     STREAM_START_DTTM BETWEEN first_stream AND     _same_day
                AND     lower(country_cd) = 'united states') )