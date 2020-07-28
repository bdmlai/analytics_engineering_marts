/*
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
{{
  config({
		 'schema': 'fds_kntr',	
	     "materialized": 'view',"tags": 'Phase4B',"persist_docs": {'relation' : true, 'columns' : true}

        })
}}
select week_start_date,
 src_country,src_channel,
series_name,src_demographic_group,src_demographic_age,hd_flag ,
rat_value,
viewing_hours,
duration_hours,
count_telecast,
Weekly_Cumulative_Audience
from  {{ref('intm_kntr_wwe_program_rating')}}