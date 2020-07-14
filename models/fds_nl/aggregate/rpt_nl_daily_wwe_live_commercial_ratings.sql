--WWE Live Commercial Ratings Report Table (daily)

/*
*************************************************************************************************************************************************
   Date        : 06/12/2020
   Version     : 1.0
   TableName   : rpt_nl_daily_wwe_live_commercial_ratings
   Schema	   : fds_nl
   Contributor : Rahul Chandran
   Description : WWE Live Commercial Ratings Daily Report table consist of rating details of all WWE Live - RAW, SD, NXT Programs referencing from Commercial Viewership Ratings table on daily-basis
*************************************************************************************************************************************************
*/


{{
  config({
		'schema': 'fds_nl',
		"pre-hook": ["delete from fds_nl.rpt_nl_daily_wwe_live_commercial_ratings where etl_insert_rec_dttm > (select max(etl_insert_rec_dttm) from fds_nl.fact_nl_commercial_viewership_ratings)"],
	     "materialized": 'incremental','tags': "Phase4B", "persist_docs": {'relation' : true, 'columns' : true}
  })
}}
SELECT
    broadcast_date_id,
    broadcast_date,
    b.cal_mth_num                        AS broadcast_month_num,
    b.mth_abbr_nm                        AS broadcast_month_nm,
    b.cal_year_qtr_num                   AS broadcast_quarter_num,
    substring(b.cal_year_qtr_desc, 5, 2) AS broadcast_quarter_nm,
    b.cal_year                           AS broadcast_year,
    src_broadcast_network_id,
    src_playback_period_cd,
    src_demographic_group,
    src_program_id,
    avg_viewing_hours_units,
    natl_comm_clockmts_avg_audience_proj_000,
    natl_comm_clockmts_avg_audience_proj_pct,
    natl_comm_clockmts_cvg_area_avg_audience_proj_pct,
    natl_comm_clockmts_duration,
    'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_4B' AS etl_batch_id,
    'bi_dbt_user_prd'                                   AS etl_insert_user_id,
    CURRENT_TIMESTAMP                                   AS etl_insert_rec_dttm,
    NULL                                                AS etl_update_user_id,
    CAST( NULL AS TIMESTAMP)                            AS etl_update_rec_dttm
FROM
    {{source('fds_nl','fact_nl_commercial_viewership_ratings')}} a
LEFT JOIN
    {{source('cdm','dim_date')}} b
ON
    a.broadcast_date_id = b.dim_date_id
WHERE 
       (src_broadcast_network_id, src_program_id) IN ((5,
                                                       296881),
                                                      (5, 339681),
                                                      (5, 436999),
                                                      (81, 898521),
                                                      (10433, 1000131)) 
{% if is_incremental() %}
	 and a.etl_insert_rec_dttm  >  coalesce ((select max(etl_insert_rec_dttm) from {{this}}), '1900-01-01 00:00:00')  
{% endif %}