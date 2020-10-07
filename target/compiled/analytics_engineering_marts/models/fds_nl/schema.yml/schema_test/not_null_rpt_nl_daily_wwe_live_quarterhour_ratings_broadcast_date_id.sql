



select count(*) as validation_errors
from "entdwdb"."fds_nl"."rpt_nl_daily_wwe_live_quarterhour_ratings"
where broadcast_date_id is null

