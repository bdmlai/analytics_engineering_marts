--WWE ratings table

/*
*************************************************************************************************************************************************
   Date        : 06/12/2020
   Version     : 1.0
   TableName   : rpt_nl_daily_wwe_program_ratings
   Schema	   : fds_nl
   Contributor : Rahul Chandran
   Description : WWE Program Ratings Daily Report Table consist of rating details of all WWE Programs referencing from Program Viewership Ratings table on daily-basis
*************************************************************************************************************************************************
*/


{{
  config({
		'schema': 'fds_nl',
		"pre-hook": ["delete from fds_nl.rpt_nl_daily_wwe_program_ratings where etl_insert_rec_dttm > (select max(etl_insert_rec_dttm) from fds_nl.fact_nl_program_viewership_ratings)"],
	     "materialized": 'incremental',"tags": 'Phase4B', "persist_docs": {'relation' : true, 'columns' : true}
  })
}}

SELECT
    a.broadcast_date_id,
    a.broadcast_date,
    d.cal_year_week_num                  AS broadcast_cal_week,
    d.mth_abbr_nm                        AS broadcast_cal_month,
    substring(d.cal_year_qtr_desc, 5, 2) AS broadcast_cal_quarter,
    d.cal_year                           AS broadcast_cal_year,
    -- financial year data is deriving from cdm.dim_date table here..
    CASE
        WHEN date_part(YEAR, d.cal_year_mon_week_begin_date) <> date_part(YEAR,
            d.cal_year_mon_week_end_date)
        THEN 1
        ELSE d.cal_year_week_num_mon
    END AS broadcast_fin_week,
    CASE
        WHEN date_part(YEAR, d.cal_year_mon_week_begin_date) <> date_part(YEAR,
            d.cal_year_mon_week_end_date)
        THEN 'jan'
        ELSE
              (
              SELECT
                  g.mth_abbr_nm
              FROM
                  {{source('cdm','dim_date')}} g
              WHERE
                  g.full_date = d.cal_year_mon_week_begin_date)
    END AS broadcast_fin_month,
    CASE
        WHEN date_part(YEAR, d.cal_year_mon_week_begin_date) <> date_part(YEAR,
            d.cal_year_mon_week_end_date)
        THEN 'q1'
        WHEN date_part(MONTH, d.cal_year_mon_week_begin_date) BETWEEN 1 AND 3
        THEN 'q1'
        WHEN date_part(MONTH, d.cal_year_mon_week_begin_date) BETWEEN 4 AND 6
        THEN 'q2'
        WHEN date_part(MONTH, d.cal_year_mon_week_begin_date) BETWEEN 7 AND 9
        THEN 'q3'
        WHEN date_part(MONTH, d.cal_year_mon_week_begin_date) BETWEEN 10 AND 12
        THEN 'q4'
        ELSE 'er'
    END AS broadcast_fin_quarter,
    CASE
        WHEN date_part(YEAR, d.cal_year_mon_week_begin_date) <> date_part(YEAR,
            d.cal_year_mon_week_end_date)
        THEN date_part(YEAR, d.cal_year_mon_week_end_date)::INT
        ELSE d.cal_year
    END AS broadcast_fin_year,
    a.src_broadcast_network_id,
    e.broadcast_network_name,
    a.src_playback_period_cd,
    a.src_demographic_group,
    a.src_program_id,
    a.src_daypart_cd,
    f.src_daypart_name,
    a.program_telecast_rpt_starttime,
    a.program_telecast_rpt_endtime,
    a.src_total_duration,
    a.avg_audience_proj_000,
    a.avg_audience_pct,
    a.avg_audience_pct_nw_cvg_area,
    a.avg_viewing_hours_units,
    'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_4B' AS etl_batch_id,
    'bi_dbt_user_uat'                                   AS etl_insert_user_id,
    CURRENT_TIMESTAMP                                   AS etl_insert_rec_dttm,
    NULL                                                AS etl_update_user_id,
    CAST( NULL AS TIMESTAMP)                            AS etl_update_rec_dttm
FROM
    (
        --RAW telecasts broken down are to be rolled up as one with start time as min(start time),
        -- end time as max(end time) and all the metrics except --VH are rolled up as time-duration
        -- based avg - ( metric 1 * duration 1 + metric 2* duration* 2 + metric 3 * duration 3) /
        -- (duration 1 + duration --2 + duration 3) here..
        SELECT
            broadcast_date_id,
            broadcast_date,
            dim_nl_broadcast_network_id,
            src_broadcast_network_id,
            src_playback_period_cd,
            src_demographic_group,
            src_program_id,
            dim_nl_daypart_id,
            src_daypart_cd,
            MIN(program_telecast_rpt_starttime) AS program_telecast_rpt_starttime,
            MAX(program_telecast_rpt_endtime)   AS program_telecast_rpt_endtime,
            SUM(src_total_duration)             AS src_total_duration,
            (SUM(avg_audience_proj_000 * src_total_duration)/NULLIF(SUM(nvl2(avg_audience_proj_000,
            src_total_duration, NULL)), 0)) AS avg_audience_proj_000,
            (SUM(avg_audience_pct * src_total_duration)/NULLIF(SUM(nvl2(avg_audience_pct,
            src_total_duration, NULL)), 0)) AS avg_audience_pct,
            (SUM(avg_audience_pct_nw_cvg_area * src_total_duration)/NULLIF(SUM(nvl2
            (avg_audience_pct_nw_cvg_area, src_total_duration, NULL)), 0)) AS
                                            avg_audience_pct_nw_cvg_area,
            SUM(avg_viewing_hours_units) AS avg_viewing_hours_units
        FROM
            {{source('fds_nl','fact_nl_program_viewership_ratings')}} b
        JOIN
            (
                SELECT
                    dim_nl_series_id
                FROM
                    {{source('fds_nl','dim_nl_series')}}
                WHERE
                    wwe_series_qualifier = 'WWE') c
        ON
            b.dim_nl_series_id = c.dim_nl_series_id
        WHERE
            src_program_id = 296881 -- filter on program id to consider only RAW telecasts
		{% if is_incremental() %}
			and b.etl_insert_rec_dttm  >  coalesce ((select max(etl_insert_rec_dttm) from {{this}}), '1900-01-01 00:00:00') 
		{% endif %}
        GROUP BY
            1,2,3,4,5,6,7,8,9
        UNION
        SELECT
            broadcast_date_id,
            broadcast_date,
            dim_nl_broadcast_network_id,
            src_broadcast_network_id,
            src_playback_period_cd,
            src_demographic_group,
            src_program_id ,
            dim_nl_daypart_id,
            src_daypart_cd,
            program_telecast_rpt_starttime,
            program_telecast_rpt_endtime,
            src_total_duration,
            avg_audience_proj_000,
            avg_audience_pct,
            avg_audience_pct_nw_cvg_area,
            avg_viewing_hours_units
        FROM
            {{source('fds_nl','fact_nl_program_viewership_ratings')}} b
        JOIN
            (
                SELECT
                    dim_nl_series_id
                FROM
                    {{source('fds_nl','dim_nl_series')}}
                WHERE
                    wwe_series_qualifier = 'WWE') c
        ON
            b.dim_nl_series_id = c.dim_nl_series_id
        WHERE
            src_program_id <> 296881 --filter on program is not equal to 296881 selects all telecast other than RAW
		{% if is_incremental() %}
			and b.etl_insert_rec_dttm  >  coalesce ((select max(etl_insert_rec_dttm) from {{this}}), '1900-01-01 00:00:00')  
		{% endif %}
    )a
LEFT JOIN
    {{source('cdm','dim_date')}} d
ON
    a.broadcast_date_id = d.dim_date_id
LEFT JOIN
    {{source('fds_nl','dim_nl_broadcast_network')}} e
ON
    a.dim_nl_broadcast_network_id = e.dim_nl_broadcast_network_id
LEFT JOIN
    {{source('fds_nl','dim_nl_daypart')}} f
ON
    a.dim_nl_daypart_id = f.dim_nl_daypart_id 