{{config(severity='error')}}
SELECT broadcast_date_id, broadcast_date, broadcast_month_num, broadcast_month_nm, 
broadcast_quarter_num, broadcast_quarter_nm, broadcast_year, src_broadcast_network_id, 
src_playback_period_cd, src_demographic_group, src_program_id, interval_starttime, 
interval_endtime, interval_duration, avg_viewing_hours_units, avg_audience_proj_000, 
avg_audience_pct, avg_pct_nw_cvg_area,count(*) FROM fds_nl.rpt_nl_daily_wwe_live_quarterhour_ratings group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
having count(*)>1
