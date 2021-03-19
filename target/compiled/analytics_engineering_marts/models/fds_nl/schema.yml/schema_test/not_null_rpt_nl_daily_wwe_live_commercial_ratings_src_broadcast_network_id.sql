
    
    



select count(*) as validation_errors
from "entdwdb"."fds_nl"."rpt_nl_daily_wwe_live_commercial_ratings"
where src_broadcast_network_id is null


