{{
  config({
	"materialized": 'view',
	"post-hook": ["COMMENT ON COLUMN fds_nl.vw_aggr_nl_quarterly_fin_wwe_live_program_ratings.broadcast_fin_quarter IS 'Broadcast Financial Quarter'",
				   "COMMENT ON COLUMN fds_nl.vw_aggr_nl_quarterly_fin_wwe_live_program_ratings.broadcast_fin_year IS 'Broadcast Financial Year'",
				   "COMMENT ON COLUMN fds_nl.vw_aggr_nl_quarterly_fin_wwe_live_program_ratings.src_broadcast_network_id IS 'A unique numerical identifier for an individual programming originator.'",
				   "COMMENT ON COLUMN fds_nl.vw_aggr_nl_quarterly_fin_wwe_live_program_ratings.src_playback_period_cd IS 'A comma separated list of data streams. Time-shifted viewing from DVR Playback or On-demand content with the same commercial load.• Live (Live - Includes viewing that occurred during the live airing).• Live+SD (Live + Same Day -Includes all playback that occurred within the same day of the liveairing).• Live+3 (Live + 3 Days - Includes all playback that occurred within three days of the live airing).• Live+7 (Live + 7 Days - Includes all playback that occurred within seven days of the live airing).'",
				   "COMMENT ON COLUMN fds_nl.vw_aggr_nl_quarterly_fin_wwe_live_program_ratings.src_demographic_group IS 'A comma separated list of demographic groups (e.g. Females 18 to 49 and Males 18 - 24 input as F18-49,M18-24).'",
				   "COMMENT ON COLUMN fds_nl.vw_aggr_nl_quarterly_fin_wwe_live_program_ratings.src_program_id IS 'A unique numerical identifier for an individual program nam.'",
				   "COMMENT ON COLUMN fds_nl.vw_aggr_nl_quarterly_fin_wwe_live_program_ratings.avg_audience_proj_000 IS 'Total U.S. Average Audience Projection (000) (The projected number of households tuned or persons viewing a program/originator/daypart during the average minute, expressed in thousands.)'",
				   "COMMENT ON COLUMN fds_nl.vw_aggr_nl_quarterly_fin_wwe_live_program_ratings.avg_audience_pct IS 'Total U.S. Average Audience Percentage (The percentage of the target demographic viewing the average minute of the selected program or time period within the total U.S.)'",
				   "COMMENT ON COLUMN fds_nl.vw_aggr_nl_quarterly_fin_wwe_live_program_ratings.avg_audience_pct_nw_cvg_area IS 'Coverage Area Average Audience Percent (The percentage of the target demographic viewing the average minute of a selected program or time period within a network’s coverage area.)'",
				   "COMMENT ON COLUMN fds_nl.vw_aggr_nl_quarterly_fin_wwe_live_program_ratings.avg_viewing_hours_units IS 'Derived Average Viewing Hours in minutes'",
				   "COMMENT ON COLUMN fds_nl.vw_aggr_nl_quarterly_fin_wwe_live_program_ratings.number_of_airings IS 'Number of airings'"
				]
	})
}}

--WWE LIVE program ratings (finance team's quarterly definition)

/*
*************************************************************************************************************************************************
   Date        : 06/12/2020
   Version     : 1.0
   TableName   : vw_aggr_nl_quarterly_fin_wwe_live_program_ratings
   Schema	   : fds_nl
   Contributor : Hima Dasan
   Description : WWE Live Program Rating Quarterly Finance Aggregate View consist of rating details of all WWE Live - RAW, SD, NXT Programs to be rolled up from WWE Program Ratings Daily Report Table on quarterly-basis
*************************************************************************************************************************************************
*/


select broadcast_fin_quarter, broadcast_fin_year, src_broadcast_network_id, src_playback_period_cd, src_demographic_group, src_program_id,
--Duration Weighted Averages are taking here for avg_audience_proj_000, avg_audience_pct and avg_audience_pct_nw_cvg_area..
(sum(avg_audience_proj_000 * src_total_duration)/nullif(sum(nvl2(avg_audience_proj_000, src_total_duration, null)), 0)) as avg_audience_proj_000,
(sum(avg_audience_pct * src_total_duration)/nullif(sum(nvl2(avg_audience_pct, src_total_duration, null)), 0)) as avg_audience_pct,
(sum(avg_audience_pct_nw_cvg_area * src_total_duration)/nullif(sum(nvl2(avg_audience_pct_nw_cvg_area, src_total_duration, null)), 0)) as avg_audience_pct_nw_cvg_area, sum(avg_viewing_hours_units) as avg_viewing_hours_units, count(*) as number_of_airings
from {{ref('rpt_nl_daily_wwe_program_ratings')}}
where (src_broadcast_network_id, src_program_id) in ((5, 296881), (5, 339681), (5, 436999), (81, 898521), (10433, 1000131))
group by 1,2,3,4,5,6 