{{
  config({
    "materialized": "incremental"
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
        avg_audience_proj_000, avg_audience_pct, avg_pct_nw_cvg_area
        FROM {{ref('fact_nl_quaterhour_viewership_ratings')}} a
     LEFT JOIN {{ref('dim_date')}} b ON a.broadcast_date_id = b.dim_date_id
    WHERE (src_broadcast_network_id, src_program_id) IN ((5, 296881), (5, 339681), (5, 436999), (81, 898521), (10433, 1000131));