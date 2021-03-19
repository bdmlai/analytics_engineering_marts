
select week_start_date, src_country, src_channel, src_property, demographic, hd_flag, 
duration_hours, rat_value, viewing_hours, telecasts_count, weekly_cumulative_audience as average_weekly_cumulative_audience_000
from "entdwdb"."fds_kntr"."aggr_kntr_weekly_competitive_program_ratings"