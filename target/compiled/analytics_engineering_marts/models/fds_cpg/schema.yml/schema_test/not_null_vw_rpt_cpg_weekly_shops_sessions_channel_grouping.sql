
    
    



select count(*) as validation_errors
from "entdwdb"."fds_cpg"."vw_rpt_cpg_weekly_shops_sessions"
where channel_grouping is null


