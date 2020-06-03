{{
  config({
    "materialized": "incremental"
  })
}}
select a.broadcast_date_id, a.broadcast_date, d.cal_year_week_num as broadcast_cal_week, d.mth_abbr_nm as broadcast_cal_month,
substring(d.cal_year_qtr_desc, 5, 2) as broadcast_cal_quarter, d.cal_year as broadcast_cal_year,
-- financial year data is deriving from cdm.dim_date table here..
case when date_part(year, d.cal_year_mon_week_begin_date) <> date_part(year, d.cal_year_mon_week_end_date)
        then 1
	 else d.cal_year_week_num_mon 
end as broadcast_fin_week,
case when date_part(year, d.cal_year_mon_week_begin_date) <> date_part(year, d.cal_year_mon_week_end_date)
        then 'jan'
	 else (select g.mth_abbr_nm from {{source('cdm', 'dim_date') }} g where g.full_date = d.cal_year_mon_week_begin_date)
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
a.src_total_duration, a.avg_audience_proj_000, a.avg_audience_pct, a.avg_audience_pct_nw_cvg_area, a.avg_viewing_hours_units,
null,null,null,null,null  
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
from {{source('fds_nl','fact_nl_program_viewership_ratings')}}  b
join (select dim_nl_series_id from {{source('fds_nl', 'dim_nl_series') }} where wwe_series_qualifier = 'WWE') c
on b.dim_nl_series_id = c.dim_nl_series_id
where src_program_id = 296881 
group by 1,2,3,4,5,6,7,8,9
union
select broadcast_date_id, broadcast_date, dim_nl_broadcast_network_id, src_broadcast_network_id, src_playback_period_cd, 
src_demographic_group, src_program_id , dim_nl_daypart_id, src_daypart_cd, program_telecast_rpt_starttime, program_telecast_rpt_endtime,
src_total_duration, avg_audience_proj_000, avg_audience_pct, avg_audience_pct_nw_cvg_area, avg_viewing_hours_units
from {{source('fds_nl','fact_nl_program_viewership_ratings')}} b
join (select dim_nl_series_id from {{source('fds_nl', 'dim_nl_series') }} where wwe_series_qualifier = 'WWE') c
on b.dim_nl_series_id = c.dim_nl_series_id
where src_program_id <> 296881
)a
left join {{source('cdm', 'dim_date') }}  d on a.broadcast_date_id = d.dim_date_id
left join {{source('fds_nl', 'dim_nl_broadcast_network') }}   e on a.dim_nl_broadcast_network_id = e.dim_nl_broadcast_network_id
left join  {{source('fds_nl', 'dim_nl_daypart')}} f on a.dim_nl_daypart_id = f.dim_nl_daypart_id