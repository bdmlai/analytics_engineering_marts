
    
    



select count(*) as validation_errors
from "entdwdb"."fds_cpg"."aggr_cpg_daily_shops_sessions"
where channel_grouping is null


