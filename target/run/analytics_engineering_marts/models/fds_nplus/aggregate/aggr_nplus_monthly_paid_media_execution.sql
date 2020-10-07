
      

    insert into "entdwdb"."fds_nplus"."aggr_nplus_monthly_paid_media_execution" ("week", "country", "vehicle", "level2", "level3", "metric", "impressions", "spend", "clicks", "data_category", "audience", "ppv_name", "ppv_type", "etl_batch_id", "etl_insert_user_id", "etl_insert_rec_dttm", "etl_update_user_id", "etl_update_rec_dttm")
    (
       select "week", "country", "vehicle", "level2", "level3", "metric", "impressions", "spend", "clicks", "data_category", "audience", "ppv_name", "ppv_type", "etl_batch_id", "etl_insert_user_id", "etl_insert_rec_dttm", "etl_update_user_id", "etl_update_rec_dttm"
       from "aggr_nplus_monthly_paid_media_execution__dbt_tmp20200930005137028815"
    );
  