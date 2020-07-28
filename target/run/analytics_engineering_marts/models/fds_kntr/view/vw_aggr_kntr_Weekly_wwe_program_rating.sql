

  create view "entdwdb"."fds_kntr"."vw_aggr_kntr_weekly_wwe_program_rating__dbt_tmp" as (
    with __dbt__CTE__intm_kntr_wwe_program_rating as (

select  week_start_date,
 extract(month from week_Start_date)  as month,
        extract(quarter from week_Start_date) as quarter,
        extract(year from week_Start_date) as year,
 src_country,src_channel,
series_name,src_demographic_group,src_demographic_age,hd_flag ,
(sum(rat_value*duration_mins))/(nullif(sum(nvl2(rat_value,duration_mins,null)),0)) as rat_value,
sum(duration_mins) as total_duration_mins,
sum(watched_mins/60) as viewing_hours,
sum(duration_mins/60.00) as duration_hours,
count(*) as count_telecast,
sum(aud) as Weekly_Cumulative_Audience
from "entdwdb"."fds_kntr"."fact_kntr_wwe_telecast_data"  a
  group by 1,2,3,4,5,6,7,8,9,10
)/*
*************************************************************************************************************************************************
   Date        : 07/21/2020
   Version     : 1.0
   TableName   : vw_aggr_kntr_Weekly_wwe_program_rating
   Schema	   : fds_nl
   Contributor : Hima Dasan
   Description : WWE  rogram Rating Weekly  Aggregate View consist of rating details of all WWE  Programs to be rolled up from wwe telecast data on weekly-basis
*************************************************************************************************************************************************

## Maintenance Log
* Date : 06/19/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of Phase 4b Project.
*/

select week_start_date,
 src_country,src_channel,
series_name,src_demographic_group,src_demographic_age,hd_flag ,
rat_value,
viewing_hours,
duration_hours,
count_telecast,
Weekly_Cumulative_Audience
from  __dbt__CTE__intm_kntr_wwe_program_rating
  ) ;
