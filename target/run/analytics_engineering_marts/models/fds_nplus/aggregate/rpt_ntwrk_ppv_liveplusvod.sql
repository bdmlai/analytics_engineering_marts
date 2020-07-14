
      

    insert into "entdwdb"."fds_nplus"."rpt_ntwrk_ppv_liveplusvod" ("asset_id", "production_id", "event", "event_name", "event_date", "start_time", "end_time", "platform", "views", "us_views", "minutes", "per_us_views", "prev_month_views", "prev_month_event", "prev_year_views", "prev_year_event", "monthly_per_change_views", "yearly_per_change_views", "duration", "overall_rank", "yearly_rank", "tier", "monthly_color", "yearly_color", "choose_ppv", "event_brand", "report_name", "series_name", "account", "url", "content_wweid", "data_level")
    (
       select "asset_id", "production_id", "event", "event_name", "event_date", "start_time", "end_time", "platform", "views", "us_views", "minutes", "per_us_views", "prev_month_views", "prev_month_event", "prev_year_views", "prev_year_event", "monthly_per_change_views", "yearly_per_change_views", "duration", "overall_rank", "yearly_rank", "tier", "monthly_color", "yearly_color", "choose_ppv", "event_brand", "report_name", "series_name", "account", "url", "content_wweid", "data_level"
       from "rpt_ntwrk_ppv_liveplusvod__dbt_tmp20200714151632263504"
    );
  