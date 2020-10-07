



select count(*) as validation_errors
from "entdwdb"."fds_kntr"."rpt_kntr_schedule_vh_data"
where dim_date_id is null

