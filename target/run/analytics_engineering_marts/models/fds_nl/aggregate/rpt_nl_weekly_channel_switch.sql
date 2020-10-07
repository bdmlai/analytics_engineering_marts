
      

    insert into "entdwdb"."fds_nl"."rpt_nl_weekly_channel_switch" ("broadcast_date", "coverage_area", "src_market_break", "src_broadcast_network_name", "src_demographic_group", "time_minute", "mc_us_aa000", "absolute_set_off_off_air", "absolute_stay", "stay_percent", "absolute_switch", "switch_percent", "switch_percent_rank", "etl_batch_id", "etl_insert_user_id", "etl_insert_rec_dttm", "etl_update_user_id", "etl_update_rec_dttm")
    (
       select "broadcast_date", "coverage_area", "src_market_break", "src_broadcast_network_name", "src_demographic_group", "time_minute", "mc_us_aa000", "absolute_set_off_off_air", "absolute_stay", "stay_percent", "absolute_switch", "switch_percent", "switch_percent_rank", "etl_batch_id", "etl_insert_user_id", "etl_insert_rec_dttm", "etl_update_user_id", "etl_update_rec_dttm"
       from "rpt_nl_weekly_channel_switch__dbt_tmp20200827041007693251"
    );
  