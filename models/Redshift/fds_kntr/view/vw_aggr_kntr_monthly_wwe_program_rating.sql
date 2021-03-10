/*
*************************************************************************************************************************************************
   Date        : 07/21/2020
   Version     : 1.0
   TableName   : vw_aggr_kntr_monthly_wwe_program_rating
   Schema	   : fds_nl
   Contributor : Hima Dasan
   Description : WWE  rogram Rating monthly  Aggregate View consist of rating details of all WWE  Programs to be rolled up from wwe telecast data on monthly-basis
*************************************************************************************************************************************************

## Maintenance Log
* Date : 06/19/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of Phase 4b Project.
*/
{{
  config({
		 'schema': 'fds_kntr',	
	     "materialized": 'view',"tags": 'Phase4B',"persist_docs": {'relation' : true, 'columns' : true}

        })
}}
  select TO_CHAR(TO_DATE (month::text, 'MM'), 'Mon') as month,
  year,src_country,src_channel,
series_name,src_demographic_group,src_demographic_age,hd_flag ,
(sum(rat_value*total_duration_mins))/(nullif(sum(nvl2(rat_value,total_duration_mins,null)),0)) as rat_value,
sum(viewing_hours) as viewing_hours,
sum(duration_hours) as duration_hours,
sum(count_telecast) as count_telecast,
avg(Weekly_Cumulative_Audience) as average_weekly_cumulative_audience_000
from {{ref('intm_kntr_wwe_program_rating')}} group by 1,2,3,4,5,6,7,8
