/*
*******************************************************************************************
Date : 06/12/2020
Version : 1.0
TableName : rpt_nl_daily_wwe_program_ratings
Schema : fds_nl
Contributor : Rahul Chandran
Description : WWE Live Program Ratings Daily Report table
***************************************************************************************************
Updates
TYPE JIRA DEVELOPER DATE DESCRIPTION
----- --------- ----- -----------
/* 8/06/2020 | Remya K Nair | Enhancements program_rating report table -Exclude all the repeats while calculating the live viewership - TAB-2106 */
/* 8/14/2020 | Remya K Nair | Enhancements program_rating report table - TAB-2105 */
/* 12/07/2020 | Remya K Nair | Enhancement Added 11 columns to program_rating report table - PSTA-1349 */

COMMENT ON TABLE fds_nl.rpt_nl_daily_wwe_program_ratings
IS
    'WWE Live Program Ratings Daily Report table';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_date_id
IS
    'Broadcast Date ID Field';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_date
IS
    'Derived dates based on the viewing period; before 6 am morning hours is the previous date broadcast hour';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.orig_broadcast_date_id
IS
    'References dim_date_id primary key';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_cal_week_begin_date
IS
    'Calendar Year Week Begin Date based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_cal_week_end_date
IS
    'Calendar Year Week End Date based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_cal_week_num
IS
    'Calendar Year Week Number based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_cal_month_num
IS
    'Calendar Year Month Number based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_cal_month_nm
IS
    'Calendar Year Month based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_cal_quarter
IS
    'Calendar Year Quarter based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_cal_year
IS
    'Calendar Year based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_fin_week_begin_date
IS
    'Financial Year Week Begin Date based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_fin_week_end_date
IS
    'Financial Year Week End Date based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_fin_week_num
IS
    'Financial Year Week Number based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_fin_month_num
IS
    'Financial Year Month Number based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_fin_month_nm
IS
    'Financial Year Month based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_fin_quarter
IS
    'Financial Year Quarter based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_fin_year
IS
    'Financial Year based on Broadcast Date';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.src_broadcast_network_id
IS
    'A unique numerical identifier for an individual programming originator.';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.broadcast_network_name
IS
    'Broadcast netowrk Name or the channel name or view source name';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.src_playback_period_cd
IS
    'A comma separated list of data streams.';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.src_demographic_group
IS
    'A comma separated list of demographic groups (e.g. Females 18 to 49 and Males 18 - 24 input as F18-49,M18-24).'
    ;
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.src_program_id
IS
    'A unique numerical identifier for an individual program name';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.src_series_name
IS
    'A unique identifier for an individual program name.';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.src_program_attributes
IS
    'Special attributes attached to programs that let you select a set of days on which selected programs air and also permit dropping of specials, breakouts, or programs of less than five minutes duration. For syndication, you can also drop long-term programs.';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.program_aired_weekday
IS
    'The day of the week during which the program aired (e.g. THURSDAY).';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.telecast_trackage_name
IS
    'A unique identifier provided by the client to track each telecast (e.g. NCIS MARA TH 3P tracks airings of NCIS that occurred on Thursdays at 3PM).';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.src_episode_id
IS
    'A unique numerical identifier for an individual episode of a program.';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.src_episode_number
IS
    'The sequential episode number within the program series.';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.src_episode_title
IS
    'Source provided Title for the episode of a series';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.src_daypart_cd
IS
    'A unique character identifier for an individual daypart';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.src_daypart_name
IS
    'A unique character identifier for an individual daypart description';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.program_telecast_rpt_starttime
IS
    'The start time of the program telecast (HH:MM).';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.program_telecast_rpt_endtime
IS
    'The end time of the program telecast (HH:MM).';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.src_total_duration
IS
    'The duration of the program/telecast airing (minutes).';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.avg_audience_proj_000
IS
    'Total U.S. Average Audience Projection (000) (The projected number of households tuned or persons viewing a program/originator/daypart during the average minute, expressed in thousands.)'
    ;
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.avg_audience_pct
IS
    'Total U.S. Average Audience Percentage (The percentage of the target demographic viewing the average minute of the selected program or time period within the total U.S.)'
    ;
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.avg_audience_pct_nw_cvg_area
IS
    'Coverage Area Average Audience Percent (The percentage of the target demographic viewing the average minute of a selected program or time period within a network’s coverage area.)'
    ;
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.viewing_hours
IS
    'Derived Average Viewing Hours ';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.viewing_hours_000
IS
    'Derived Average persons viewing hours ';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.avg_audience_proj_units
IS
    'Total U.S. Average Audience Projection (Units) (The projected number of households tuned or persons viewing a program/originator/daypart during the average minute.)';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.share_pct
IS
    'Total U.S. Share Percentage (The percentage of households tuning or persons viewing that are tuned to a specific program/originator/daypart.)';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.share_pct_nw_cvg_area
IS
    'Coverage Area Share Percent (The percentage of households or persons using television (HUT/PUT) viewing a particular program or time period within a specific coverage area.)';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.etl_batch_id
IS
    'Unique ID of DBT Job used to insert the record';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.etl_insert_user_id
IS
    'Unique ID of the DBT user that was used to insert the record';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.etl_insert_rec_dttm
IS
    'Date Time information on when the DBT inserted the record';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.etl_update_user_id
IS
    'Unique ID of the DBT user which was used to update the record manually';
COMMENT ON COLUMN fds_nl.rpt_nl_daily_wwe_program_ratings.etl_update_rec_dttm
IS
    'Date Time information on when the record was updated';