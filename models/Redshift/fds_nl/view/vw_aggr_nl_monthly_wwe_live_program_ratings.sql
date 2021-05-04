
/*
*************************************************************************************************************************************************
   Date        : 07/21/2020
   Version     : 1.0
   TableName   : vw_aggr_nl_monthly_wwe_live_program_ratings
   Schema	   : fds_nl
   Contributor : Remya K Nair
   Description : WWE Live Program Rating Monthly Aggregate View consist of rating details of all WWE Live - RAW, SD, NXT Programs to be rolled up from WWE Program Ratings Daily Report Table on monthly-basis
*************************************************************************************************************************************************
*/
{{
  config({
	'schema': 'fds_nl',"materialized": 'view',"tags": 'rpt_nl_daily_wwe_program_ratings',"persist_docs": {'relation' : true, 'columns' : true},'post-hook': 'grant select on {{ this }} to public'
	})
}}

SELECT broadcast_cal_month_nm as broadcast_month, broadcast_cal_year as broadcast_year, 
src_broadcast_network_id, src_playback_period_cd, src_demographic_group, src_program_id,src_series_name,
--Duration Weighted Averages are taking here for avg_audience_proj_000, avg_audience_pct and avg_audience_pct_nw_cvg_area..
(sum(avg_audience_proj_000 * src_total_duration)/nullif(sum(nvl2(avg_audience_proj_000, src_total_duration, null)), 0)) as avg_audience_proj_000,
(sum(avg_audience_pct * src_total_duration)/nullif(sum(nvl2(avg_audience_pct, src_total_duration, null)), 0)) as avg_audience_pct,
(sum(avg_audience_pct_nw_cvg_area * src_total_duration)/nullif(sum(nvl2(avg_audience_pct_nw_cvg_area, src_total_duration, null)), 0)) as avg_audience_pct_nw_cvg_area,
 sum(viewing_hours) as tot_viewing_minutes, count(*) as number_of_airings
FROM {{ref('rpt_nl_daily_wwe_program_ratings')}}
WHERE (src_broadcast_network_id, src_program_id)
   IN ((5, 296881), (5, 339681), (5, 436999), (81, 898521), (10433, 1000131),(22,201245))
GROUP BY 1,2,3,4,5,6,7 

