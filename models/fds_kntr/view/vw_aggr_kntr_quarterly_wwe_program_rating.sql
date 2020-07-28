/*
*************************************************************************************************************************************************
   Date        : 07/17/2020
   Version     : 1.0
   TableName   : vw_aggr_kntr_quarterly_wwe_program_rating
   Schema	   : fds_nl
   Contributor : Hima Dasan
   Description : WWE  rogram Rating quarterly  Aggregate View consist of rating details of all WWE  Programs to be rolled up from wwe telecast data on quaterly-basis
*************************************************************************************************************************************************

## Maintenance Log
* Date : 07/17/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of Phase 4b Project.
*/
{{
  config({
		 'schema': 'fds_kntr',	
	     "materialized": 'view',"tags": 'Phase4B',"persist_docs": {'relation' : true, 'columns' : true}

        })
}}
select 'q'+ cast(quarter as varchar) as quarter,
year,src_country,src_channel,
series_name,src_demographic_group,src_demographic_age,hd_flag ,
(sum(rat_value*total_duration_mins))/(nullif(sum(nvl2(rat_value,total_duration_mins,null)),0)) as rat_value,
sum(viewing_hours) as viewing_hours,
sum(duration_hours) as duration_hours,
sum(count_telecast) as count_telecast,
avg(Weekly_Cumulative_Audience) as Average_Weekly_Cumulative_Audience
from {{ref('intm_kntr_wwe_program_rating')}} group by 1,2,3,4,5,6,7,8
