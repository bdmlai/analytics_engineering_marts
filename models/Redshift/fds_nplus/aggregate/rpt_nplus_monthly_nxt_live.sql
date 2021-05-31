 {{ config({ "schema": 'fds_nplus', "materialized": 'table',"tags": 'nxt_live_kickoff',"persist_docs": {'relation' : true, 'columns' : true}, "post-hook" : ['grant select on {{this}} to public', "drop table fds_nplus.intm_nplus_live_manual_nxt", "drop table fds_nplus.intm_nplus_prior_change_live_nxt", "drop table fds_nplus.intm_nplus_live_manual_base_nxt", "drop table fds_nplus.intm_nplus_live_nwk_unique_viewers_nxt", "drop table fds_nplus.intm_nplus_live_nwk_unique_viewers_t1_nxt", "drop table fds_nplus.intm_nplus_live_dotcom_plays_nxt", "drop table fds_nplus.intm_nplus_live_manual_base1_nxt", "drop table fds_nplus.intm_nplus_live_manual_base_total_nxt", "drop table fds_nplus.intm_nplus_live_consolidation_nxt", "drop table fds_nplus.intm_nplus_live_final_nxt"] }) }}
 
 
 SELECT
         asset_id                                                                                                         ,
         production_id                                                                                                    ,
         event                                                                                                            ,
         event_name                                                                                                       ,
         event_date                                                                                                       ,
         start_time                                                                                                       ,
         end_time                                                                                                         ,
         platform                                                                                                         ,
         views                                                                                                            ,
         us_views                                                                                                         ,
         minutes                                                                                                          ,
         per_us_views                                                                                                     ,
         prev_month_views                                                                                                 ,
         prev_month_event                                                                                                 ,
         prev_year_views                                                                                                  ,
         prev_year_event                                                                                                  ,
         monthly_per_change_views                                                                                         ,
         yearly_per_change_views                                                                                          ,
         duration                                                                                                         ,
         overall_rank                                                                                                     ,
         yearly_rank                                                                                                      ,
         tier                                                                                                             ,
         monthly_color                                                                                                    ,
         yearly_color                                                                                                     ,
         choose_ppv                                                                                                       ,
         event_brand                                                                                                      ,
         report_name                                                                                                      ,
         series_name                                                                                                      ,
         account                                                                                                          ,
         url                                                                                                              ,
         content_wwe_id AS content_wweid                                                                                  ,
         data_level                                                                                                       ,
         'DBT_'+TO_CHAR(convert_timezone('AMERICA/NEW_YORK', SYSDATE),'YYYY_MM_DD_HH_MI_SS')+'_NXT'    etl_batch_id       ,
         'bi_dbt_user_prd'                                                                          AS etl_insert_user_id ,
         convert_timezone('AMERICA/NEW_YORK', SYSDATE)                                              AS etl_insert_rec_dttm,
         NULL                                                                                       AS etl_update_user_id ,
         CAST( NULL AS TIMESTAMP)                                                                   AS etl_update_rec_dttm
 FROM
         {{ ref("intm_nplus_live_final_nxt") }}
		 
		 