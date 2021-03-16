--QH ratings for WWE program (monthly)

/*
*************************************************************************************************************************************************
   Date        : 06/12/2020
   Version     : 1.0
   TableName   : vw_aggr_nl_monthly_wwe_live_quarterhour_ratings
   Schema	   : fds_nl
   Contributor : Sudhakar Andugula
   Description : WWE Live Quarter Hour Rating Monthly Aggregate View consist of rating details of all WWE Live - RAW, SD, NXT Programs to be rolled up from WWE Live Quarter Hour Ratings Daily Report Table on monthly-basis
**************************************************************************************************************************************************
*/
 
 

SELECT broadcast_month_nm broadcast_month, 
       broadcast_year, 
       src_broadcast_network_id,
       src_playback_period_cd,
       src_demographic_group,
       src_program_id,
       interval_starttime,
       interval_endtime,
--Duration Weighted Averages are taking for avg_audience_proj_000, avg_audience_pct and avg_pct_nw_cvg_area here..
(sum(avg_audience_proj_000 * interval_duration)/nullif(sum(nvl2(avg_audience_proj_000, interval_duration, null)), 0)) as avg_audience_proj_000,
(sum(avg_audience_pct * interval_duration)/nullif(sum(nvl2(avg_audience_pct, interval_duration, null)), 0)) as avg_audience_pct,
(sum(avg_pct_nw_cvg_area * interval_duration)/nullif(sum(nvl2(avg_pct_nw_cvg_area, interval_duration, null)), 0)) as avg_pct_nw_cvg_area, 
sum(avg_viewing_hours_units) as tot_viewing_minutes
FROM "entdwdb"."fds_nl"."rpt_nl_daily_wwe_live_quarterhour_ratings"
GROUP BY  1,2,3,4,5,6,7,8