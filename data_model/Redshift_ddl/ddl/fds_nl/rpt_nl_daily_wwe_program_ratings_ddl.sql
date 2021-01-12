/*
************************************************************************************ 
    Date: 6/12/2020
    Version: 1.0
    Description: First Draft
    Contributor: Rahul Chandran
    Adl' notes:  
    updated comments
**************************************************************************************/
/* date      | by      |    Details   */
/* 8/06/2020 | Remya K Nair | Enhancements program_rating report table -Exclude all the repeats while calculating the live viewership - TAB-2106 */
/* 8/14/2020 | Remya K Nair | Enhancements program_rating report table - TAB-2105 */
/* 12/07/2020 | Remya K Nair | Enhancement Added 11 columns to program_rating report table - PSTA-1349 */

CREATE TABLE
    fds_nl.rpt_nl_daily_wwe_program_ratings
    (
        broadcast_date_id INTEGER ENCODE LZO,
        broadcast_date DATE ENCODE LZO,
	orig_broadcast_date_id INTEGER ENCODE ZSTD,
        broadcast_cal_week_begin_date DATE ENCODE LZO,
        broadcast_cal_week_end_date DATE ENCODE LZO,
        broadcast_cal_week_num SMALLINT ENCODE LZO,
        broadcast_cal_month_num SMALLINT ENCODE LZO,
        broadcast_cal_month_nm CHARACTER VARYING(20) ENCODE LZO,
        broadcast_cal_quarter CHARACTER VARYING(20) ENCODE LZO,
        broadcast_cal_year SMALLINT ENCODE LZO,
        broadcast_fin_week_begin_date DATE ENCODE LZO,
        broadcast_fin_week_end_date DATE ENCODE LZO,
        broadcast_fin_week_num BIGINT ENCODE LZO,
        broadcast_fin_month_num BIGINT ENCODE LZO,
        broadcast_fin_month_nm CHARACTER VARYING(20) ENCODE LZO,
	broadcast_fin_quarter CHARACTER VARYING(16383) ENCODE LZO,
        broadcast_fin_year BIGINT ENCODE LZO,
        src_broadcast_network_id BIGINT ENCODE LZO,
        broadcast_network_name CHARACTER VARYING(255) ENCODE LZO,
        src_playback_period_cd CHARACTER VARYING(255) ENCODE LZO,
        src_demographic_group CHARACTER VARYING(255) ENCODE LZO,
        src_program_id BIGINT ENCODE BYTEDICT,
        src_series_name CHARACTER VARYING(255) ENCODE LZO,
	src_program_attributes CHARACTER VARYING(255) ENCODE LZO,
	program_aired_weekday CHARACTER VARYING(255) ENCODE LZO,
	telecast_trackage_name CHARACTER VARYING(255) ENCODE LZO,
	src_episode_id BIGINT ENCODE ZSTD,
	src_episode_number BIGINT ENCODE ZSTD,
	src_episode_title CHARACTER VARYING(2000),		
        src_daypart_cd CHARACTER VARYING(255) ENCODE LZO,
        src_daypart_name CHARACTER VARYING(255) ENCODE LZO,
        program_telecast_rpt_starttime CHARACTER VARYING(255) ENCODE LZO,
        program_telecast_rpt_endtime CHARACTER VARYING(255) ENCODE LZO,
        src_total_duration BIGINT ENCODE BYTEDICT,
        avg_audience_proj_000 BIGINT ENCODE AZ64,
        avg_audience_pct NUMERIC(20,2) ENCODE BYTEDICT,
        avg_audience_pct_nw_cvg_area NUMERIC(20,2) ENCODE BYTEDICT,
        viewing_hours BIGINT ENCODE AZ64,
	viewing_hours_000 BIGINT ENCODE ZSTD,
	avg_audience_proj_units BIGINT ENCODE ZSTD,
	share_pct NUMERIC(20,2) ENCODE ZSTD,
	share_pct_nw_cvg_area NUMERIC(20,2) ENCODE ZSTD,		
        etl_batch_id CHARACTER VARYING(100) ENCODE LZO,
        etl_insert_user_id CHARACTER VARYING(100) ENCODE LZO,
        etl_insert_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        etl_update_user_id CHARACTER VARYING(50) ENCODE LZO,
        etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        
    );
