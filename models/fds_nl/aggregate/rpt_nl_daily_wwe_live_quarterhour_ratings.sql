--WWE Live QH Ratings Report Table (daily)

/*
*************************************************************************************************************************************************
   Date        : 06/12/2020
   Version     : 1.0
   TableName   : rpt_nl_daily_wwe_live_quarterhour_ratings
   Schema	   : fds_nl
   Contributor : Sudhakar Andugula
   Description : WWE Live Quarter Hour Ratings Daily Report table consist of rating details of all WWE Live - RAW, SD, NXT Programs referencing from Quarter Hour Viewership Ratings Table on daily-basis 
*************************************************************************************************************************************************
*/


{{
  config({
		"pre-hook": ["delete from fds_nl.rpt_nl_daily_wwe_program_ratings where etl_insert_rec_dttm > (select max(etl_insert_rec_dttm) from fds_nl.fact_nl_commercial_viewership_ratings)"],
	     "materialized": 'incremental'
  })
}}

SELECT   broadcast_date_id, 
         broadcast_date,
         b.cal_mth_num as broadcast_month_num,
         b.mth_abbr_nm as broadcast_month_nm, 
         b.cal_year_qtr_num as broadcast_quarter_num, 
        substring(b.cal_year_qtr_desc, 5, 2) as broadcast_quarter_nm, 
        b.cal_year as broadcast_year,
        src_broadcast_network_id, src_playback_period_cd,
        src_demographic_group, src_program_id, interval_starttime, 
        interval_endtime, interval_duration, avg_viewing_hours_units,
        avg_audience_proj_000, avg_audience_pct, avg_pct_nw_cvg_area,
		'DBT_'+to_char(sysdate,'YYYY_MM_DD_HH_MI_SS')+'_4B' as etl_batch_id, 'bi_dbt_user_uat' as etl_insert_user_id, current_timestamp as etl_insert_rec_dttm, NULL as etl_update_user_id, 
CAST( NULL AS TIMESTAMP) as etl_update_rec_dttm
        FROM {{source('fds_nl','fact_nl_quaterhour_viewership_ratings')}} a
     LEFT JOIN {{source('cdm','dim_date')}} b ON a.broadcast_date_id = b.dim_date_id
    WHERE (src_broadcast_network_id, src_program_id) IN ((5, 296881), (5, 339681), (5, 436999), (81, 898521), (10433, 1000131))		
{% if is_incremental() %}
	 and a.etl_insert_rec_dttm  >  (select max(etl_insert_rec_dttm) from {{this}}) 
{% endif %}
