
      

    insert into "entdwdb"."fds_nplus"."aggr_nplus_monthly_owned_media_execution" ("week", "country", "vehicle", "level2", "level3", "metric", "exposure", "data_category", "audience", "ppv_name", "ppv_type", "etl_batch_id", "etl_insert_user_id", "etl_insert_rec_dttm", "etl_update_user_id", "etl_update_rec_dttm")
    (
       select "week", "country", "vehicle", "level2", "level3", "metric", "exposure", "data_category", "audience", "ppv_name", "ppv_type", "etl_batch_id", "etl_insert_user_id", "etl_insert_rec_dttm", "etl_update_user_id", "etl_update_rec_dttm"
       from "aggr_nplus_monthly_owned_media_execution__dbt_tmp20200929060908410763"
    );
  