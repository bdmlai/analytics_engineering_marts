--Commercial ratings for WWE program(quarterly)

/*
*************************************************************************************************************************************************
   Date        : 06/12/2020
   Version     : 1.0
   TableName   : vw_aggr_nl_yearly_wwe_live_commercial_ratings
   Schema	   : fds_nl
   Contributor : Rahul Chandran
   Description : WWE Live Commercial Rating Yearly Aggregate View consist of rating details of all WWE Live - RAW, SD, NXT Programs to be rolled up from WWE Live Commercial Ratings Daily Report Table on yearly-basis
*************************************************************************************************************************************************
*/

 

select  broadcast_year, src_broadcast_network_id, src_playback_period_cd, src_demographic_group, src_program_id,
--Duration Weighted Averages are taking for natl_comm_clockmts_avg_audience_proj_000, natl_comm_clockmts_avg_audience_proj_pct and --natl_comm_clockmts_cvg_area_avg_audience_proj_pct here..
(sum(natl_comm_clockmts_avg_audience_proj_000 * natl_comm_clockmts_duration)/
nullif(sum(nvl2(natl_comm_clockmts_avg_audience_proj_000, natl_comm_clockmts_duration, null)), 0)) as natl_comm_clockmts_avg_audience_proj_000,
(sum(natl_comm_clockmts_avg_audience_proj_pct * natl_comm_clockmts_duration)/
nullif(sum(nvl2(natl_comm_clockmts_avg_audience_proj_pct, natl_comm_clockmts_duration, null)), 0)) as natl_comm_clockmts_avg_audience_proj_pct,
(sum(natl_comm_clockmts_cvg_area_avg_audience_proj_pct * natl_comm_clockmts_duration)/
nullif(sum(nvl2(natl_comm_clockmts_cvg_area_avg_audience_proj_pct, natl_comm_clockmts_duration, null)), 0))
as natl_comm_clockmts_cvg_area_avg_audience_proj_pct, sum(avg_viewing_hours_units) as tot_viewing_minutes
from  "entdwdb"."fds_nl"."rpt_nl_daily_wwe_live_commercial_ratings"
group by 1,2,3,4,5