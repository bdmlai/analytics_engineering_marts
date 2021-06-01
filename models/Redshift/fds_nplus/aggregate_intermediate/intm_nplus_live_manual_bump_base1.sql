{{ config({ "schema": 'fds_nplus', "materialized": 'ephemeral',"tags": 'bump_liveshow' }) }}
SELECT
        event      ,
        event_name ,
        event_brand,
        report_name,
        series_name,
        event_date ,
        start_time ,
        end_time   ,
        platform   ,
        CASE WHEN platform = 'Network' THEN
                        (
                                SELECT
                                        unique_viewers
                                FROM
                                        {{ ref("intm_nplus_live_nwk_unique_viewers_bump") }}) ELSE views END AS views,
        minutes                                                                                                      ,
        peak_concurrents                                                                                             ,
        prev_week_views                                                                                              ,
        prev_week_event                                                                                              ,
        production_id                                                                                                ,
        content_wwe_id                                                                                               ,
        data_level                                                                                                   ,
        weekly_flag
FROM
        {{ ref("intm_nplus_live_manual_bump_base") }}