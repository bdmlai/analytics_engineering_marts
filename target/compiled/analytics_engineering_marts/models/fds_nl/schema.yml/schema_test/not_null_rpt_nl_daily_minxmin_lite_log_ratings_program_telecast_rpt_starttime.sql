



select count(*) as validation_errors
from "entdwdb"."fds_nl"."rpt_nl_daily_minxmin_lite_log_ratings"
where program_telecast_rpt_starttime is null

