/*
*************************************************************************************************************************************************
   Date        : 07/26/2020
   Version     : 1.0
   TableName   : vw_aggr_kntr_monthly_wwe_program_rating_schedule
   Schema	   : fds_kntr
   Contributor : Remya K Nair
   Description : vw_aggr_kntr_monthly_wwe_program_rating_schedule view  consist of  Monthly RAW,SD,NXT and PPVs ratings for Live & Nth runs on monthly-basis
*************************************************************************************************************************************************
*/
{{
  config({
		 'schema': 'fds_kntr',	
	     "materialized": 'view',"tags": 'Phase4B',"persist_docs": {'relation' : true, 'columns' : true}

        })
}}
select  TO_CHAR(TO_DATE (cal_month::text, 'MM'), 'Mon') as month,
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
        
from  {{ref('intm_kntr_wwe_program_rating_schedule')}}
group by 1,2,3,4,5,6,7,8,9,10