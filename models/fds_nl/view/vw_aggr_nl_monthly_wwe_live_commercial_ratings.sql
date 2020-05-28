 {{
  config({
	"materialized": 'view',
	"post-hook": ["COMMENT ON COLUMN fds_nl.vw_aggr_nl_monthly_wwe_live_commercial_ratings.broadcast_month IS 'Broadcast Month name';
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_monthly_wwe_live_commercial_ratings.broadcast_year IS 'Broadcast  year';
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_monthly_wwe_live_commercial_ratings.src_broadcast_network_id IS 'A unique numerical identifier for an individual programming originator.';
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_monthly_wwe_live_commercial_ratings.src_playback_period_cd IS 'A comma separated list of data streams. Time-shifted viewing from DVR Playback or On-demand content with the same commercial load.';  
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_monthly_wwe_live_commercial_ratings.src_demographic_group IS 'A comma separated list of demographic groups (e.g. Females 18 to 49 and Males 18 - 24 input as F18-49,M18-24).'; 
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_monthly_wwe_live_commercial_ratings.src_program_id IS 'A unique numerical identifier for an individual program name. '; 
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_monthly_wwe_live_commercial_ratings.natl_comm_clockmts_avg_audience_proj_000 IS '"National Commercial Clock Minute Average Audience Projection (000) (The projected number of households tuned or persons viewing the average qualified commercial minute of the selected program within the total U.S., expressed in thousands.)"';
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_monthly_wwe_live_commercial_ratings.natl_comm_clockmts_avg_audience_proj_pct IS 'National Commercial Clock Minute Average Audience Percentage (The percentage of the target demographic viewing the average qualified commercial minute of the selected program within the total U.S.)';
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_monthly_wwe_live_commercial_ratings.natl_comm_clockmts_cvg_area_avg_audience_proj_pct IS 'National Commercial Clock Minute Coverage Area Average Audience Percent (The percentage of the target demographic viewing the average qualified commercial minute of a selected program within a network’s coverage area.)';  
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_monthly_wwe_live_commercial_ratings.avg_viewing_hours_units IS 'Derived Average Viewing Hours in minutes ';"]
	})
}}

--Commercial ratings for WWE program (monthly)

/*
*************************************************************************************************************************************************
   Date        : 06/12/2020
   Version     : 1.0
   TableName   : vw_aggr_nl_monthly_wwe_live_commercial_ratings
   Schema	   : fds_nl
   Contributor : Rahul Chandran
   Description : WWE Live Commercial Rating Monthly Aggregate View consist of rating details of all WWE Live - RAW, SD, NXT Programs to be rolled up from WWE Live Commercial Ratings Daily Report Table on monthly-basis
*************************************************************************************************************************************************
*/

select broadcast_month_nm as broadcast_month, broadcast_year, 
src_broadcast_network_id, src_playback_period_cd, src_demographic_group, src_program_id,
--Duration Weighted Averages are taking for natl_comm_clockmts_avg_audience_proj_000, natl_comm_clockmts_avg_audience_proj_pct and --natl_comm_clockmts_cvg_area_avg_audience_proj_pct here..
(sum(natl_comm_clockmts_avg_audience_proj_000 * natl_comm_clockmts_duration)/
nullif(sum(nvl2(natl_comm_clockmts_avg_audience_proj_000, natl_comm_clockmts_duration, null)), 0)) as natl_comm_clockmts_avg_audience_proj_000,
(sum(natl_comm_clockmts_avg_audience_proj_pct * natl_comm_clockmts_duration)/
nullif(sum(nvl2(natl_comm_clockmts_avg_audience_proj_pct, natl_comm_clockmts_duration, null)), 0)) as natl_comm_clockmts_avg_audience_proj_pct,
(sum(natl_comm_clockmts_cvg_area_avg_audience_proj_pct * natl_comm_clockmts_duration)/
nullif(sum(nvl2(natl_comm_clockmts_cvg_area_avg_audience_proj_pct, natl_comm_clockmts_duration, null)), 0))
as natl_comm_clockmts_cvg_area_avg_audience_proj_pct, sum(avg_viewing_hours_units) as avg_viewing_hours_units
from  {{ref('rpt_nl_daily_wwe_live_commercial_ratings')}}
group by 1,2,3,4,5,6