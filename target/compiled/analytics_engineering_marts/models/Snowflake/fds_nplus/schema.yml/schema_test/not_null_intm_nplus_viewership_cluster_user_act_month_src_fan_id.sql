
    
    



select count(*) as validation_errors
from "entdwdb"."dt_stage"."intm_nplus_viewership_cluster_user_act_month"
where src_fan_id is null


