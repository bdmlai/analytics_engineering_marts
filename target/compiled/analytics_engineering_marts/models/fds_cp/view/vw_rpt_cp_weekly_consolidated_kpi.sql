
SELECT granularity, platform, type, metric, year, month, week, start_date, end_date, value, prev_year, 
prev_year_week, prev_year_start_date, prev_year_end_date, prev_year_value FROM "entdwdb"."fds_cp"."rpt_cp_weekly_consolidated_kpi"
union
SELECT granularity, platform, type, metric, year, month, week, start_date, end_date, value, prev_year, 
prev_year_week, prev_year_start_date, prev_year_end_date, prev_year_value FROM "entdwdb"."fds_nl"."rpt_tv_weekly_consolidated_kpi"