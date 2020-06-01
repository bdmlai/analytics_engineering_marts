{{
  config({
    "materialized": "incremental"
  })
}}
select broadcast_date_id, broadcast_date, b.cal_mth_num as broadcast_month_num, b.mth_abbr_nm as broadcast_month_nm, 
b.cal_year_qtr_num as broadcast_quarter_num, substring(b.cal_year_qtr_desc, 5, 2) as broadcast_quarter_nm,  b.cal_year as broadcast_year,
src_broadcast_network_id, src_playback_period_cd, src_demographic_group, src_program_id, avg_viewing_hours_units,  natl_comm_clockmts_avg_audience_proj_000, natl_comm_clockmts_avg_audience_proj_pct, natl_comm_clockmts_cvg_area_avg_audience_proj_pct, 
<<<<<<< HEAD
natl_comm_clockmts_duration,null,null,null,null,null from {{ref('fact_nl_commercial_viewership_ratings')}} a
=======
natl_comm_clockmts_duration from {{ref('fact_nl_commercial_viewership_ratings')}} a
>>>>>>> f00efc65e348df7fc2d6a4c6a748ea6e1cb5d026
left join {{ref('dim_date')}} b on a.broadcast_date_id = b.dim_date_id
where (src_broadcast_network_id, src_program_id) in ((5, 296881), (5, 339681), (5, 436999), (81, 898521), (10433, 1000131))