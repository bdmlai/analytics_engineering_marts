{{ config({ "schema": 'fds_nplus', "materialized": 'ephemeral',"tags": 'bump_liveshow' }) }}
SELECT
        event                                                                                                            ,
        event_name                                                                                                       ,
        event_brand                                                                                                      ,
        report_name                                                                                                      ,
        series_name                                                                                                      ,
        event_date                                                                                                       ,
        start_time                                                                                                       ,
        end_time                                                                                                         ,
        platform                                                                                                         ,
        views                                                                                                            ,
        minutes                                                                                                          ,
        peak_concurrents                                                                                                 ,
        prev_week_views                                                                                                  ,
        prev_week_event                                                                                                  ,
        weekly_per_change_views                                                                                          ,
        duration                                                                                                         ,
        overall_rank                                                                                                     ,
        weekly_color                                                                                                     ,
        choose_event                                                                                                     ,
        production_id                                                                                                    ,
        content_wweid                                                                                                    ,
        data_level                                                                                                       ,
        weekly_flag                                                                                                      ,
        'DBT_'+TO_CHAR(convert_timezone('AMERICA/NEW_YORK', SYSDATE),'YYYY_MM_DD_HH_MI_SS')+'_BMP'    etl_batch_id       ,
        'bi_dbt_user_prd'                                                                          AS etl_insert_user_id ,
        convert_timezone('AMERICA/NEW_YORK', SYSDATE)                                              AS etl_insert_rec_dttm,
        NULL                                                                                       AS etl_update_user_id ,
        CAST( NULL AS TIMESTAMP)                                                                   AS etl_update_rec_dttm
FROM
        {{ ref("intm_nplus_live_final_bump") }}