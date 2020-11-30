create table dwh_read_write.rpt_nl_daily_wwe_program_ratings
(broadcast_date_id  INTEGER ENCODE AZ64,
broadcast_date  DATE ENCODE AZ64,
broadcast_cal_week SMALLINT ENCODE AZ64,
broadcast_cal_month CHARACTER VARYING(20) ENCODE ZSTD,
broadcast_cal_quarter CHARACTER VARYING(20) ENCODE ZSTD,
broadcast_cal_year SMALLINT ENCODE AZ64,
broadcast_fin_week SMALLINT ENCODE AZ64,
broadcast_fin_month CHARACTER VARYING(20) ENCODE ZSTD,
broadcast_fin_quarter CHARACTER VARYING(20) ENCODE ZSTD,
broadcast_fin_year SMALLINT ENCODE AZ64,
src_broadcast_network_id BIGINT ENCODE AZ64,
broadcast_network_name CHARACTER VARYING(255) ENCODE ZSTD,
src_playback_period_cd CHARACTER VARYING(255) ENCODE ZSTD,
src_demographic_group CHARACTER VARYING(255) ENCODE ZSTD,
src_program_id BIGINT ENCODE AZ64,
src_daypart_cd CHARACTER VARYING(255) ENCODE ZSTD,
src_daypart_name CHARACTER VARYING(255) ENCODE ZSTD,
program_telecast_rpt_starttime CHARACTER VARYING(255) ENCODE ZSTD,
program_telecast_rpt_endtime CHARACTER VARYING(255) ENCODE ZSTD,
src_total_duration BIGINT ENCODE AZ64,
avg_audience_proj_000 BIGINT ENCODE AZ64,
avg_audience_pct NUMERIC(20,2) ENCODE AZ64,
avg_audience_pct_nw_cvg_area NUMERIC(20,2) ENCODE AZ64,
avg_viewing_hours_units BIGINT ENCODE AZ64
);


insert into  dwh_read_write.rpt_nl_daily_wwe_program_ratings
(select a.broadcast_date_id, a.broadcast_date, d.cal_year_week_num as broadcast_cal_week, d.mth_abbr_nm as broadcast_cal_month,
substring(d.cal_year_qtr_desc, 5, 2) as broadcast_cal_quarter, d.cal_year as broadcast_cal_year,
-- financial year data is deriving from cdm.dim_date table here..
case when date_part(year, d.cal_year_mon_week_begin_date) <> date_part(year, d.cal_year_mon_week_end_date)
        then 1
	 else d.cal_year_week_num_mon 
end as broadcast_fin_week,
case when date_part(year, d.cal_year_mon_week_begin_date) <> date_part(year, d.cal_year_mon_week_end_date)
        then 'jan'
	 else (select g.mth_abbr_nm from cdm.dim_date g where g.full_date = d.cal_year_mon_week_begin_date)
end as broadcast_fin_month,
case when date_part(year, d.cal_year_mon_week_begin_date) <> date_part(year, d.cal_year_mon_week_end_date)
        then 'q1'
	 when date_part(month, d.cal_year_mon_week_begin_date) between 1 and 3
	    then 'q1'
	 when date_part(month, d.cal_year_mon_week_begin_date) between 4 and 6
	    then 'q2'
	 when date_part(month, d.cal_year_mon_week_begin_date) between 7 and 9
	    then 'q3'
	 when date_part(month, d.cal_year_mon_week_begin_date) between 10 and 12
	    then 'q4'
	  else 'er'
end as broadcast_fin_quarter,
case when date_part(year, d.cal_year_mon_week_begin_date) <> date_part(year, d.cal_year_mon_week_end_date)
        then date_part(year, d.cal_year_mon_week_end_date)::int
	 else d.cal_year 
end as broadcast_fin_year,
a.src_broadcast_network_id, e.broadcast_network_name, a.src_playback_period_cd, a.src_demographic_group, a.src_program_id, a.src_daypart_cd,
f.src_daypart_name, a.program_telecast_rpt_starttime, a.program_telecast_rpt_endtime,
a.src_total_duration, a.avg_audience_proj_000, a.avg_audience_pct, a.avg_audience_pct_nw_cvg_area, a.avg_viewing_hours_units  
from
(
--RAW telecasts broken down are to be rolled up as one with start time as min(start time), end time as max(end time) and all the metrics except --VH are rolled up as time-duration based avg - ( metric 1 * duration 1 + metric 2* duration* 2 + metric 3 * duration 3) /(duration 1 + duration --2 + duration 3) here..
select broadcast_date_id, broadcast_date, dim_nl_broadcast_network_id, src_broadcast_network_id, src_playback_period_cd, 
src_demographic_group, src_program_id, dim_nl_daypart_id, src_daypart_cd,
min(program_telecast_rpt_starttime) as program_telecast_rpt_starttime, max(program_telecast_rpt_endtime) as program_telecast_rpt_endtime,
sum(src_total_duration) as src_total_duration,
(sum(avg_audience_proj_000 * src_total_duration)/nullif(sum(nvl2(avg_audience_proj_000, src_total_duration, null)), 0)) as avg_audience_proj_000,
(sum(avg_audience_pct * src_total_duration)/nullif(sum(nvl2(avg_audience_pct, src_total_duration, null)), 0)) as avg_audience_pct,
(sum(avg_audience_pct_nw_cvg_area * src_total_duration)/nullif(sum(nvl2(avg_audience_pct_nw_cvg_area, src_total_duration, null)), 0)) as avg_audience_pct_nw_cvg_area, sum(avg_viewing_hours_units) as avg_viewing_hours_units
from fds_nl.fact_nl_program_viewership_ratings b
join (select dim_nl_series_id from fds_nl.dim_nl_series where wwe_series_qualifier = 'WWE') c
on b.dim_nl_series_id = c.dim_nl_series_id
where src_program_id = 296881 
group by 1,2,3,4,5,6,7,8,9
union
select broadcast_date_id, broadcast_date, dim_nl_broadcast_network_id, src_broadcast_network_id, src_playback_period_cd, 
src_demographic_group, src_program_id , dim_nl_daypart_id, src_daypart_cd, program_telecast_rpt_starttime, program_telecast_rpt_endtime,
src_total_duration, avg_audience_proj_000, avg_audience_pct, avg_audience_pct_nw_cvg_area, avg_viewing_hours_units
from fds_nl.fact_nl_program_viewership_ratings b
join (select dim_nl_series_id from fds_nl.dim_nl_series where wwe_series_qualifier = 'WWE') c
on b.dim_nl_series_id = c.dim_nl_series_id
where src_program_id <> 296881
)a
left join cdm.dim_date d on a.broadcast_date_id = d.dim_date_id
left join fds_nl.dim_nl_broadcast_network e on a.dim_nl_broadcast_network_id = e.dim_nl_broadcast_network_id
left join fds_nl.dim_nl_daypart f on a.dim_nl_daypart_id = f.dim_nl_daypart_id);

COMMENT ON TABLE dwh_read_write.rpt_nl_daily_wwe_program_ratings
IS
    'WWE Live Program Ratings Daily Report table';
COMMENT ON COLUMN dwh_read_write.rpt_nl_daily_wwe_program_ratings.broadcast_date_id
IS
    'Broadcast Date ID Field';
COMMENT ON COLUMN dwh_read_write.rpt_nl_daily_wwe_program_ratings.broadcast_date
IS
    'Derived dates based on the viewing period; before 6 am morning hours is the preious date broadcast hour';
	
	COMMENT ON COLUMN dwh_read_write.rpt_nl_daily_wwe_program_ratings.broadcast_cal_week
IS
    'Broadcast Calendar Week Number';
COMMENT ON COLUMN dwh_read_write.rpt_nl_daily_wwe_program_ratings.broadcast_cal_month
IS
    'Broadcast Calendar Month';
COMMENT ON COLUMN dwh_read_write.rpt_nl_daily_wwe_program_ratings.broadcast_cal_quarter
IS
    'Broadcast Calendar Quarter';
COMMENT ON COLUMN dwh_read_write.rpt_nl_daily_wwe_program_ratings.broadcast_cal_year
IS
    'Broadcast Calendar Year';
COMMENT ON COLUMN dwh_read_write.rpt_nl_daily_wwe_program_ratings.broadcast_fin_week
IS
    'Broadcast Financial Week Number';	
COMMENT ON COLUMN dwh_read_write.rpt_nl_daily_wwe_program_ratings.broadcast_fin_month
IS
    'Broadcast Financial Month';
	COMMENT ON COLUMN dwh_read_write.rpt_nl_daily_wwe_program_ratings.broadcast_fin_quarter
IS
    'Broadcast Financial Quarter';
	COMMENT ON COLUMN dwh_read_write.rpt_nl_daily_wwe_program_ratings.broadcast_fin_year
IS
    'Broadcast Financial Year';
COMMENT ON COLUMN dwh_read_write.rpt_nl_daily_wwe_program_ratings.src_broadcast_network_id
IS
    'A unique numerical identifier for an individual programming originator.';
	
	COMMENT ON COLUMN dwh_read_write.rpt_nl_daily_wwe_program_ratings.broadcast_network_name
IS
    'Broadcast netowrk Name or the channel name or view source name';
COMMENT ON COLUMN dwh_read_write.rpt_nl_daily_wwe_program_ratings.src_playback_period_cd
IS
    'A comma separated list of data streams. Time-shifted viewing from DVR Playback or On-demand content with the same commercial load.• Live (Live - Includes viewing that occurred during the live airing).• Live+SD (Live + Same Day -Includes all playback that occurred within the same day of the liveairing).• Live+3 (Live + 3 Days - Includes all playback that occurred within three days of the live airing).• Live+7 (Live + 7 Days - Includes all playback that occurred within seven days of the live airing).'
	 ;
COMMENT ON COLUMN dwh_read_write.rpt_nl_daily_wwe_program_ratings.src_demographic_group
IS
    'A comma separated list of demographic groups (e.g. Females 18 to 49 and Males 18 - 24 input as F18-49,M18-24).' ;
COMMENT ON COLUMN dwh_read_write.rpt_nl_daily_wwe_program_ratings.src_program_id
IS
    'A unique numerical identifier for an individual program nam.' ;
	
	COMMENT ON COLUMN dwh_read_write.rpt_nl_daily_wwe_program_ratings.src_daypart_cd
IS
    'A unique character identifier for an individual daypart';
COMMENT ON COLUMN dwh_read_write.rpt_nl_daily_wwe_program_ratings.src_daypart_name
IS
    'A unique character identifier for an individual daypart description';
COMMENT ON COLUMN dwh_read_write.rpt_nl_daily_wwe_program_ratings.program_telecast_rpt_starttime
IS
   'The start time of the program telecast (HH:MM).';
 COMMENT ON COLUMN dwh_read_write.rpt_nl_daily_wwe_program_ratings.program_telecast_rpt_endtime
IS
   'The end time of the program telecast (HH:MM).';
	
COMMENT ON COLUMN dwh_read_write.rpt_nl_daily_wwe_program_ratings.src_total_duration
IS
    'The duration of the program/telecast airing (minutes).';
COMMENT ON COLUMN dwh_read_write.rpt_nl_daily_wwe_program_ratings.avg_audience_proj_000
IS
    'Total U.S. Average Audience Projection (000) (The projected number of households tuned or persons viewing a program/originator/daypart during the average minute, expressed in thousands.)';
COMMENT ON COLUMN dwh_read_write.rpt_nl_daily_wwe_program_ratings.avg_audience_pct
IS
    'Total U.S. Average Audience Percentage (The percentage of the target demographic viewing the average minute of the selected program or time period within the total U.S.)';
COMMENT ON COLUMN dwh_read_write.rpt_nl_daily_wwe_program_ratings.avg_pct_nw_cvg_area
IS
    'Coverage Area Average Audience Percent (The percentage of the target demographic viewing the average minute of a selected program or time period within a network’s coverage area.)';  
COMMENT ON COLUMN dwh_read_write.rpt_nl_daily_wwe_program_ratings.avg_viewing_hours_units
IS
    'Derived Average Viewing Hours in minutes';