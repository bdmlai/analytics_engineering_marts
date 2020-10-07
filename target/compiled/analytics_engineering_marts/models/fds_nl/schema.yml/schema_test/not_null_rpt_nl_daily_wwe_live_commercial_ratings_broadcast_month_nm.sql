



select count(*) as validation_errors
from "entdwdb"."fds_nl"."rpt_nl_daily_wwe_live_commercial_ratings"
where broadcast_month_nm is null

