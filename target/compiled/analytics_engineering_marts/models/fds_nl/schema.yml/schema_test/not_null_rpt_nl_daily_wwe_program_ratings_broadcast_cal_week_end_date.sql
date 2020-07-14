



select count(*) as validation_errors
from "entdwdb"."fds_nl"."rpt_nl_daily_wwe_program_ratings"
where broadcast_cal_week_end_date is null

