
with dbt__CTE__INTERNAL_test as (

select broadcast_date, broadcast_cal_week_begin_date, broadcast_cal_week_end_date, 
broadcast_cal_week_num, broadcast_cal_month_nm, broadcast_cal_quarter, broadcast_cal_year,
 broadcast_fin_week_begin_date, broadcast_fin_week_end_date, broadcast_fin_week_num, 
broadcast_fin_month_nm, broadcast_fin_quarter, broadcast_fin_year, src_broadcast_network_id, 
broadcast_network_name, src_playback_period_cd, src_demographic_group, src_program_id,src_series_name, 
src_daypart_cd, src_daypart_name, program_telecast_rpt_starttime, program_telecast_rpt_endtime,avg_audience_proj_000,
avg_audience_pct, avg_audience_pct_nw_cvg_area, viewing_minutes_units,count(*)
from fds_nl.rpt_nl_daily_wwe_program_ratings
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27 having count(*)>1
)select count(*) from dbt__CTE__INTERNAL_test