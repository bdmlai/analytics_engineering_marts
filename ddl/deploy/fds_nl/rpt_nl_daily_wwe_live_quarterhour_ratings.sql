create table fds_nl.rpt_nl_daily_wwe_live_quarterhour_ratings
(
broadcast_date_id  INTEGER ENCODE AZ64,
broadcast_date  DATE ENCODE AZ64,
broadcast_month_num SMALLINT ENCODE AZ64,
broadcast_month_nm CHARACTER VARYING(20) ENCODE ZSTD,
broadcast_quarter_num SMALLINT ENCODE AZ64,
broadcast_quarter_nm CHARACTER VARYING(20) ENCODE ZSTD,
broadcast_year SMALLINT ENCODE AZ64,
src_broadcast_network_id BIGINT ENCODE AZ64,
src_playback_period_cd CHARACTER VARYING(255) ENCODE ZSTD,
src_demographic_group CHARACTER VARYING(255) ENCODE ZSTD,
src_program_id BIGINT ENCODE AZ64,
interval_starttime CHARACTER VARYING(255) ENCODE ZSTD, 
interval_endtime CHARACTER VARYING(255) ENCODE ZSTD, 
interval_duration BIGINT ENCODE AZ64,
avg_viewing_hours_units BIGINT ENCODE AZ64,
avg_audience_proj_000 BIGINT ENCODE AZ64,
avg_audience_pct NUMERIC(20,2) ENCODE AZ64,
avg_pct_nw_cvg_area NUMERIC(20,2) ENCODE AZ64
);


INSERT INTO  dwh_read_write.rpt_nl_daily_wwe_live_quarterhour_ratings
(SELECT   broadcast_date_id, 
         broadcast_date,
         b.cal_mth_num as broadcast_month_num,
         b.mth_abbr_nm as broadcast_month_nm, 
         b.cal_year_qtr_num as broadcast_quarter_num, 
        substring(b.cal_year_qtr_desc, 5, 2) as broadcast_quarter_nm, 
        b.cal_year as broadcast_year,
        src_broadcast_network_id, src_playback_period_cd,
        src_demographic_group, src_program_id, interval_starttime, 
        interval_endtime, interval_duration, avg_viewing_hours_units,
        avg_audience_proj_000, avg_audience_pct, avg_pct_nw_cvg_area
        FROM fds_nl.fact_nl_quaterhour_viewership_ratings a
     LEFT JOIN cdm.dim_date b ON a.broadcast_date_id = b.dim_date_id
    WHERE (src_broadcast_network_id, src_program_id) IN ((5, 296881), (5, 339681), (5, 436999), (81, 898521), (10433, 1000131)));

COMMENT ON TABLE fds_nl.rpt_nl_daily_wwe_live_quarterhour_ratings
IS
    'WWE Live Quarter Hour Ratings Daily Report table';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_quarterhour_ratings.broadcast_date_id
IS
    'Broadcast Date ID Field';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_quarterhour_ratings.broadcast_date
IS
    'Derived dates based on the viewing period; before 6 am morning hours is the preious date broadcast hour';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_quarterhour_ratings.broadcast_month_num
IS
    'Broadcast Month Number';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_quarterhour_ratings.broadcast_month_nm
IS
    'Broadcast Month Name';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_quarterhour_ratings.broadcast_quarter_num
IS
    'Broadcast Quarter Number';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_quarterhour_ratings.broadcast_quarter_nm
IS
    'Broadcast Quarter Name';	
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_quarterhour_ratings.broadcast_year
IS
    'Broadcast year';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_quarterhour_ratings.src_broadcast_network_id
IS
    'A unique numerical identifier for an individual programming originator.'
    ;
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_quarterhour_ratings.src_playback_period_cd
IS
    'A comma separated list of data streams. Time-shifted viewing from DVR Playback or On-demand content with the same commercial load.• Live (Live - Includes viewing that occurred during the live airing).• Live+SD (Live + Same Day -Includes all playback that occurred within the same day of the liveairing).• Live+3 (Live + 3 Days - Includes all playback that occurred within three days of the live airing).• Live+7 (Live + 7 Days - Includes all playback that occurred within seven days of the live airing).'
	 ;
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_quarterhour_ratings.src_demographic_group
IS
    'A comma separated list of demographic groups (e.g. Females 18 to 49 and Males 18 - 24 input as F18-49,M18-24).'
    ;
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_quarterhour_ratings.src_program_id
IS
    'A unique numerical identifier for an individual program nam.'
    ;
	
	COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_quarterhour_ratings.interval_starttime
IS
    'calcuated interval start time if it is quarter hour , every quarter start time will be profided'
    ;
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_quarterhour_ratings.interval_endtime
IS
    'calcuated interval end time if it is quarter hour , every quarter end time will be profided'
    ;
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_quarterhour_ratings.interval_duration
IS
    'quarter period interval duration';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_quarterhour_ratings.avg_viewing_hours_units
IS
    'Derived Average Viewing Hours in minutes';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_quarterhour_ratings.avg_audience_proj_000
IS
    'Total U.S. Average Audience Projection (000) (The projected number of households tuned or persons viewing a program/originator/daypart during the average minute, expressed in thousands.)';
 COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_quarterhour_ratings.avg_audience_pct
IS
    'Total U.S. Average Audience Percentage (The percentage of the target demographic viewing the average minute of the selected program or time period within the total U.S.)';
 COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_live_quarterhour_ratings.avg_pct_nw_cvg_area
IS
    'Coverage Area Average Audience Percent (The percentage of the target demographic viewing the average minute of a selected program or time period within a network’s coverage area.)';  