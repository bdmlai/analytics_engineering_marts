
--WWE ratings daily view (daily)

/*
*************************************************************************************************************************************************
   Date        : 06/12/2020
   Version     : 1.0
   TableName   : vw_rpt_nl_monthly_wwe_live_program_ratings
   Schema	   : fds_nl
   Contributor : Rahul Chandran
   Description : WWE Program Ratings Daily Report View consist of rating details of all WWE Programs referencing from WWE Program Ratings Daily Report table
*************************************************************************************************************************************************
*/
{{
  config({
	'schema': 'fds_nl',
	"materialized": 'view',
	"post-hook": ["COMMENT ON COLUMN fds_nl.vw_rpt_nl_daily_wwe_program_ratings.broadcast_date_id IS 'Broadcast Date ID Field';
			COMMENT ON COLUMN fds_nl.vw_rpt_nl_daily_wwe_program_ratings.broadcast_date IS 'Derived dates based on the viewing period ; before 6 am morning hours is the preious date broadcast hour'; 
			COMMENT ON COLUMN fds_nl.vw_rpt_nl_daily_wwe_program_ratings.broadcast_cal_week IS 'Broadcast Calendar Week Number';
			COMMENT ON COLUMN fds_nl.vw_rpt_nl_daily_wwe_program_ratings.broadcast_cal_month IS 'Broadcast Calendar Month';
			COMMENT ON COLUMN fds_nl.vw_rpt_nl_daily_wwe_program_ratings.broadcast_cal_quarter IS 'Broadcast Calendar Quarter';
			COMMENT ON COLUMN fds_nl.vw_rpt_nl_daily_wwe_program_ratings.broadcast_cal_year IS 'Broadcast Calendar Year';
			COMMENT ON COLUMN fds_nl.vw_rpt_nl_daily_wwe_program_ratings.src_broadcast_network_id IS 'A unique numerical identifier for an individual programming originator.';
			COMMENT ON COLUMN fds_nl.vw_rpt_nl_daily_wwe_program_ratings.broadcast_network_name IS 'Broadcast netowrk Name or the channel name or view source name';
			COMMENT ON COLUMN fds_nl.vw_rpt_nl_daily_wwe_program_ratings.src_playback_period_cd IS 'A comma separated list of data streams. Time-shifted viewing from DVR Playback or On-demand content with the same commercial load.• Live (Live - Includes viewing that occurred during the live airing).• Live+SD (Live + Same Day -Includes all playback that occurred within the same day of the liveairing).• Live+3 (Live + 3 Days - Includes all playback that occurred within three days of the live airing).• Live+7 (Live + 7 Days - Includes all playback that occurred within seven days of the live airing).';
			COMMENT ON COLUMN fds_nl.vw_rpt_nl_daily_wwe_program_ratings.src_demographic_group IS 'A comma separated list of demographic groups (e.g. Females 18 to 49 and Males 18 - 24 input as F18-49,M18-24).';
			COMMENT ON COLUMN fds_nl.vw_rpt_nl_daily_wwe_program_ratings.src_program_id IS 'A unique numerical identifier for an individual program nam.';
			COMMENT ON COLUMN fds_nl.vw_rpt_nl_daily_wwe_program_ratings.src_daypart_cd IS 'A unique character identifier for an individual daypart';
			COMMENT ON COLUMN fds_nl.vw_rpt_nl_daily_wwe_program_ratings.src_daypart_name IS 'A unique character identifier for an individual daypart description';
			COMMENT ON COLUMN fds_nl.vw_rpt_nl_daily_wwe_program_ratings.program_telecast_rpt_starttime IS 'The start time of the program telecast (HH:MM).';
			COMMENT ON COLUMN fds_nl.vw_rpt_nl_daily_wwe_program_ratings.program_telecast_rpt_endtime IS 'The end time of the program telecast (HH:MM).';
			COMMENT ON COLUMN fds_nl.vw_rpt_nl_daily_wwe_program_ratings.src_total_duration IS 'The duration of the program/telecast airing (minutes).';
			COMMENT ON COLUMN fds_nl.vw_rpt_nl_daily_wwe_program_ratings.avg_audience_proj_000 IS 'Total U.S. Average Audience Projection (000) (The projected number of households tuned or persons viewing a program/originator/daypart during the average minute, expressed in thousands.)';
			COMMENT ON COLUMN fds_nl.vw_rpt_nl_daily_wwe_program_ratings.avg_audience_pct IS 'Total U.S. Average Audience Percentage (The percentage of the target demographic viewing the average minute of the selected program or time period within the total U.S.)';
			COMMENT ON COLUMN fds_nl.vw_rpt_nl_daily_wwe_program_ratings.avg_audience_pct_nw_cvg_area IS 'Coverage Area Average Audience Percent (The percentage of the target demographic viewing the average minute of a selected program or time period within a network’s coverage area.)';
			COMMENT ON COLUMN fds_nl.vw_rpt_nl_daily_wwe_program_ratings.avg_viewing_hours_units IS 'Derived Average Viewing Hours in minutes'; 
		"]
	})
}}

select broadcast_date_id, broadcast_date, broadcast_cal_week, broadcast_cal_month, broadcast_cal_quarter, broadcast_cal_year,
		src_broadcast_network_id, broadcast_network_name, src_playback_period_cd, src_demographic_group, src_program_id,
		src_daypart_cd, src_daypart_name, program_telecast_rpt_starttime, program_telecast_rpt_endtime, src_total_duration,
		avg_audience_proj_000, avg_audience_pct, avg_audience_pct_nw_cvg_area, avg_viewing_hours_units
from {{ref('rpt_nl_daily_wwe_program_ratings')}}


