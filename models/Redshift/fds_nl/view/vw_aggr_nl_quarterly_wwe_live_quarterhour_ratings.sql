--QH ratings for WWE program(quarterly)

/*
*************************************************************************************************************************************************
   Date        : 06/12/2020
   Version     : 1.0
   TableName   : vw_aggr_nl_quarterly_wwe_live_quarterhour_ratings
   Schema	   : fds_nl
   Contributor : Sudhakar Andugula
   Description : WWE Live Quarter Hour Rating Quarterly Aggregate View consist of rating details of all WWE Live - RAW, SD, NXT Programs to be rolled up from WWE Live Quarter Hour Ratings Daily Report Table on quarterly-basis
*************************************************************************************************************************************************
*/

 {{
  config({
	'schema': 'fds_nl',
	"materialized": 'view',
	"post-hook": ["COMMENT ON COLUMN  fds_nl.vw_aggr_nl_quarterly_wwe_live_quarterhour_ratings.broadcast_quarter IS 'Broadcast Quarter Name';
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_quarterly_wwe_live_quarterhour_ratings.broadcast_year IS 'Broadcast  year';
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_quarterly_wwe_live_quarterhour_ratings.src_broadcast_network_id IS 'A unique numerical identifier for an individual programming originator.';
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_quarterly_wwe_live_quarterhour_ratings.src_playback_period_cd IS 'A comma separated list of data streams. Time-shifted viewing from DVR Playback or On-demand content with the same commercial load.';  
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_quarterly_wwe_live_quarterhour_ratings.src_demographic_group IS 'A comma separated list of demographic groups (e.g. Females 18 to 49 and Males 18 - 24 input as F18-49,M18-24).'; 
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_quarterly_wwe_live_quarterhour_ratings.src_program_id IS 'A unique numerical identifier for an individual program name. '; 
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_quarterly_wwe_live_quarterhour_ratings.interval_starttime IS 'calcuated interval start time if it is quarter hour , every quarter start time will be profided' ;
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_quarterly_wwe_live_quarterhour_ratings.interval_endtime IS 'calcuated interval end time if it is quarter hour , every quarter end time will be profided';
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_quarterly_wwe_live_quarterhour_ratings.avg_audience_proj_000 IS 'Total U.S. Average Audience Projection (000) (The projected number of households tuned or persons viewing a program/originator/daypart during the average minute, expressed in thousands.)';
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_quarterly_wwe_live_quarterhour_ratings.avg_audience_pct IS 'Total U.S. Average Audience Percentage (The percentage of the target demographic viewing the average minute of the selected program or time period within the total U.S.)';
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_quarterly_wwe_live_quarterhour_ratings.avg_pct_nw_cvg_area IS 'Coverage Area Average Audience Percent (The percentage of the target demographic viewing the average minute of a selected program or time period within a network’s coverage area.)';  
					COMMENT ON COLUMN fds_nl.vw_aggr_nl_quarterly_wwe_live_quarterhour_ratings.tot_viewing_minutes IS 'Derived Average Viewing Hours in minutes';
					"]
	})
}}
SELECT broadcast_quarter_nm broadcast_quarter, 
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
FROM {{ref('rpt_nl_daily_wwe_live_quarterhour_ratings')}}
GROUP BY  1,2,3,4,5,6,7,8