
      

    insert into "entdwdb"."fds_le"."rpt_le_weekly_consolidated_kpi" ("granularity", "platform", "type", "metric", "year", "month", "week", "start_date", "end_date", "value", "prev_year", "prev_year_week", "prev_year_start_date", "prev_year_end_date", "prev_year_value", "etl_batch_id", "etl_insert_user_id", "etl_insert_rec_dttm", "etl_update_user_id", "etl_update_rec_dttm")
    (
       select "granularity", "platform", "type", "metric", "year", "month", "week", "start_date", "end_date", "value", "prev_year", "prev_year_week", "prev_year_start_date", "prev_year_end_date", "prev_year_value", "etl_batch_id", "etl_insert_user_id", "etl_insert_rec_dttm", "etl_update_user_id", "etl_update_rec_dttm"
       from "rpt_le_weekly_consolidated_kpi__dbt_tmp20200929115245151523"
    );
  