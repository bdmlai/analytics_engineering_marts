/*
*************************************************************************************************************************************************
   Date        : 07/21/2020
   Version     : 1.0
   TableName   : vw_rpt_nl_daily_wwe_live_program_ratings
   Schema	   : fds_nl
   Contributor : Rahul Chandran
   Description : WWE Program Ratings Daily Report View consist of rating details of all WWE Programs referencing from WWE Program Ratings Daily Report table
*************************************************************************************************************************************************
*/
{{
  config({
	'schema': 'fds_nl',"materialized": 'view','tags': "Phase4B","persist_docs": {'relation' : true, 'columns' : true}
	})
}}

select  broadcast_date_id ,
        broadcast_date ,
	orig_broadcast_date_id ,
        broadcast_cal_week_begin_date ,
        broadcast_cal_week_end_date ,
        broadcast_cal_week_num ,
        broadcast_cal_month_num ,
        broadcast_cal_month_nm ,
        broadcast_cal_quarter ,
        broadcast_cal_year ,
        broadcast_fin_week_begin_date ,
        broadcast_fin_week_end_date ,
        broadcast_fin_week_num ,
        broadcast_fin_month_num ,
        broadcast_fin_month_nm ,
	broadcast_fin_quarter ,
        broadcast_fin_year ,
        src_broadcast_network_id ,
        broadcast_network_name ,
        src_playback_period_cd ,
        src_demographic_group ,
        src_program_id ,
        src_series_name ,
	src_program_attributes ,
	program_aired_weekday ,
	telecast_trackage_name ,
	src_episode_id ,
	src_episode_number ,
	src_episode_title ,		
        src_daypart_cd ,
        src_daypart_name ,
        program_telecast_rpt_starttime ,
        program_telecast_rpt_endtime ,
        src_total_duration ,
        avg_audience_proj_000 ,
        avg_audience_pct ,
        avg_audience_pct_nw_cvg_area ,
        viewing_hours  ,
	viewing_hours_000  ,
	avg_audience_proj_units ,
	share_pct ,
	share_pct_nw_cvg_area 
from {{ref('rpt_nl_daily_wwe_program_ratings')}}


