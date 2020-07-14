
      

    insert into "entdwdb"."dwh_read_write"."agg_cp_weekly_consumption_by_platform" ("platform", "monday_date", "views", "minutes_watched", "prev_views", "prev_mins", "weekly_per_change_views", "weekly_per_change_mins", "etl_batch_id", "etl_insert_user_id", "etl_insert_rec_dttm", "etl_update_user_id", "etl_update_rec_dttm")
    (
       select "platform", "monday_date", "views", "minutes_watched", "prev_views", "prev_mins", "weekly_per_change_views", "weekly_per_change_mins", "etl_batch_id", "etl_insert_user_id", "etl_insert_rec_dttm", "etl_update_user_id", "etl_update_rec_dttm"
       from "agg_cp_weekly_consumption_by_platform__dbt_tmp20200708095358761590"
    );
  