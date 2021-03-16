
    
    



select count(*) as validation_errors
from "entdwdb"."fds_cpg"."vw_rpt_cpg_monthly_shops_sessions"
where channel_grouping is null


