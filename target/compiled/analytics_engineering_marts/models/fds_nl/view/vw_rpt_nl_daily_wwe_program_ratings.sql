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


select broadcast_date, broadcast_cal_week_begin_date, broadcast_cal_week_end_date, broadcast_cal_week_num, broadcast_cal_month_nm, broadcast_cal_quarter, broadcast_cal_year, broadcast_fin_week_begin_date, broadcast_fin_week_end_date, broadcast_fin_week_num, 
broadcast_fin_month_nm, broadcast_fin_quarter, broadcast_fin_year, src_broadcast_network_id, broadcast_network_name, src_playback_period_cd, src_demographic_group, src_program_id, src_daypart_cd, src_daypart_name, program_telecast_rpt_starttime, program_telecast_rpt_endtime,avg_audience_proj_000, avg_audience_pct, avg_audience_pct_nw_cvg_area, viewing_minutes_units
from "entdwdb"."fds_nl"."rpt_nl_daily_wwe_program_ratings"