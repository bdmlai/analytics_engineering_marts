
      

    insert into "entdwdb"."fds_kntr"."rpt_kntr_schedule_vh_data" ("dim_date_id", "broadcast_date", "src_weekday", "month_name", "month_num", "modified_month", "year", "region", "src_country", "broadcast_network_prem_type", "src_channel", "demographic_type", "demographic_group_name", "src_series", "series_episode_name", "series_episode_num", "series_name", "series_type", "start_time", "end_time", "duration_mins", "hd_flag", "week_start_date", "start_time_modified", "channel_1", "program_1", "rat_value", "viewing_hours", "aud", "regional_viewing_hours", "etl_batch_id", "etl_insert_user_id", "etl_insert_rec_dttm", "etl_update_user_id", "etl_update_rec_dttm")
    (
       select "dim_date_id", "broadcast_date", "src_weekday", "month_name", "month_num", "modified_month", "year", "region", "src_country", "broadcast_network_prem_type", "src_channel", "demographic_type", "demographic_group_name", "src_series", "series_episode_name", "series_episode_num", "series_name", "series_type", "start_time", "end_time", "duration_mins", "hd_flag", "week_start_date", "start_time_modified", "channel_1", "program_1", "rat_value", "viewing_hours", "aud", "regional_viewing_hours", "etl_batch_id", "etl_insert_user_id", "etl_insert_rec_dttm", "etl_update_user_id", "etl_update_rec_dttm"
       from "rpt_kntr_schedule_vh_data__dbt_tmp20200827102510747293"
    );
  