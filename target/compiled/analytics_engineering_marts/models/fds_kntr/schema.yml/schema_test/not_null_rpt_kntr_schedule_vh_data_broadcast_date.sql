
    
    



select count(*) as validation_errors
from "entdwdb"."fds_kntr"."rpt_kntr_schedule_vh_data"
where broadcast_date is null


