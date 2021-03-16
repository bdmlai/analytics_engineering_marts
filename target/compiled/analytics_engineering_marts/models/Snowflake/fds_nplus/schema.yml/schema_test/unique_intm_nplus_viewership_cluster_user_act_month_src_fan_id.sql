
    
    



select count(*) as validation_errors
from (

    select
        src_fan_id

    from "entdwdb"."dt_stage"."intm_nplus_viewership_cluster_user_act_month"
    where src_fan_id is not null
    group by src_fan_id
    having count(*) > 1

) validation_errors


