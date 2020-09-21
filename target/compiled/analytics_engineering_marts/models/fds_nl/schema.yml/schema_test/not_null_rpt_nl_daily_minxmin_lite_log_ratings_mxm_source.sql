



select count(*) as validation_errors
from "entdwdb"."fds_nl"."rpt_nl_daily_minxmin_lite_log_ratings"
where mxm_source is null

