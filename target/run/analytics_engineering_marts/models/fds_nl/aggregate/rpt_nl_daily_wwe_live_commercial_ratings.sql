
      

    insert into "entdwdb"."fds_nl"."rpt_nl_daily_wwe_live_commercial_ratings" ("broadcast_date_id", "broadcast_date", "broadcast_month_num", "broadcast_month_nm", "broadcast_quarter_num", "broadcast_quarter_nm", "broadcast_year", "src_broadcast_network_id", "src_playback_period_cd", "src_demographic_group", "src_program_id", "avg_viewing_hours_units", "natl_comm_clockmts_avg_audience_proj_000", "natl_comm_clockmts_avg_audience_proj_pct", "natl_comm_clockmts_cvg_area_avg_audience_proj_pct", "natl_comm_clockmts_duration", "etl_batch_id", "etl_insert_user_id", "etl_insert_rec_dttm", "etl_update_user_id", "etl_update_rec_dttm")
    (
       select "broadcast_date_id", "broadcast_date", "broadcast_month_num", "broadcast_month_nm", "broadcast_quarter_num", "broadcast_quarter_nm", "broadcast_year", "src_broadcast_network_id", "src_playback_period_cd", "src_demographic_group", "src_program_id", "avg_viewing_hours_units", "natl_comm_clockmts_avg_audience_proj_000", "natl_comm_clockmts_avg_audience_proj_pct", "natl_comm_clockmts_cvg_area_avg_audience_proj_pct", "natl_comm_clockmts_duration", "etl_batch_id", "etl_insert_user_id", "etl_insert_rec_dttm", "etl_update_user_id", "etl_update_rec_dttm"
       from "rpt_nl_daily_wwe_live_commercial_ratings__dbt_tmp20200717041810807112"
    );
  