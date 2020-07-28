



select count(*) as validation_errors
from "entdwdb"."fds_kntr"."aggr_kntr_weekly_competitive_program_ratings"
where quarter is null

