
      

    insert into "entdwdb"."fds_nl"."rpt_nl_weekly_overlap_derived_4_way_oob" ("week_starting_date", "input_type", "coverage_area", "market_break", "demographic_group", "playback_period_cd", "program_combination", "p2_total_unique_reach_proj", "p2_total_unique_reach_percent", "overlap_description", "etl_batch_id", "etl_insert_user_id", "etl_insert_rec_dttm", "etl_update_user_id", "etl_update_rec_dttm")
    (
       select "week_starting_date", "input_type", "coverage_area", "market_break", "demographic_group", "playback_period_cd", "program_combination", "p2_total_unique_reach_proj", "p2_total_unique_reach_percent", "overlap_description", "etl_batch_id", "etl_insert_user_id", "etl_insert_rec_dttm", "etl_update_user_id", "etl_update_rec_dttm"
       from "rpt_nl_weekly_overlap_derived_4_way_oob__dbt_tmp20200819131638815855"
    );
  