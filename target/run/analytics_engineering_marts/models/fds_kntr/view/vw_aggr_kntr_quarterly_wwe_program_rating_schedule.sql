

  create view "entdwdb"."fds_kntr"."vw_aggr_kntr_quarterly_wwe_program_rating_schedule__dbt_tmp" as (
    with __dbt__CTE__intm_kntr_wwe_program_rating_schedule as (


select  week_Start_date,
        extract(month from week_Start_date) as cal_month,
        extract(quarter from week_Start_date) as cal_quarter,
        extract(year from week_Start_date) as cal_year,
        src_country,
        src_channel,
        series_name,
        src_demographic_group,
        src_demographic_age,
        hd_flag ,
        live_flag,
        nth_run,
        (sum(rat_value*duration_mins))/(nullif(sum(nvl2(rat_value,duration_mins,null)),0)) as rat_value,
        sum(duration_mins) as total_duration_mins,
        sum(watched_mins/60) as viewing_hours,
        sum(duration_mins/60.00) as duration_hours,
        sum(aud) as Sum_Weekly_Cumulative_Audience

from "entdwdb"."fds_kntr"."fact_kntr_wwe_telecast_data" a
where a.series_name in ('SmackDown','RAW','NXT','PPV')
  and a.nth_run is not null
 group by 1,2,3,4,5,6,7,8,9,10,11,12
)/*
*************************************************************************************************************************************************
   Date        : 07/26/2020
   Version     : 1.0
   TableName   : vw_aggr_kntr_quarterly_wwe_program_rating_schedule
   Schema	   : fds_kntr
   Contributor : Remya K Nair
   Description : vw_aggr_kntr_quarterly_wwe_program_rating_schedule view  consist of  quarterly RAW,SD,NXT and PPVs ratings for Live & Nth runs on quarterly-basis
*************************************************************************************************************************************************
*/

select 'q'+ cast(cal_quarter as varchar) as quarter,
        cal_year as year,
        src_country,
        src_channel,
        series_name,
        src_demographic_group ,
        src_demographic_age ,
        hd_flag ,
        live_flag,
        nth_run,
        (sum(rat_value*total_duration_mins))/(nullif(sum(nvl2(rat_value,total_duration_mins,null)),0)) as rat_value,
        sum(viewing_hours) as viewing_hours,
        sum(duration_hours) as duration_hours,
        count(*) as count_telecast,
        avg(Sum_Weekly_Cumulative_Audience) as Average_Weekly_Cumulative_Audience
from  __dbt__CTE__intm_kntr_wwe_program_rating_schedule
group by 1,2,3,4,5,6,7,8,9,10
  ) ;
