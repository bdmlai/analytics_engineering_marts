{{
  config({
		"schema": 'fds_nl',
	    "materialized": 'incremental'
  })
}}

select 		a.broadcast_date_id, 
			a.broadcast_date, 
			a.orig_broadcast_date_id, 
			d.cal_year_week_begin_date as broadcast_cal_week_begin_date, 
			d.cal_year_week_end_date as broadcast_cal_week_end_date, 
			d.cal_year_week_num as broadcast_cal_week_num, 
			d.cal_mth_num as broadcast_cal_month_num, 
			d.mth_abbr_nm as broadcast_cal_month_nm, 
			substring(d.cal_year_qtr_desc, 5, 2) as broadcast_cal_quarter, 
			d.cal_year as broadcast_cal_year,
			e.fin_year_week_begin_date as broadcast_fin_week_begin_date, 
			e.fin_year_week_end_date as broadcast_fin_week_end_date,
			e.financial_year_week_number as broadcast_fin_week_num, 
			e.financial_month_number as broadcast_fin_month_num, 
			e.financial_month_name as broadcast_fin_month_nm, 
			e.financial_quarter as broadcast_fin_quarter, 
			e.financial_year as broadcast_fin_year,
			a.src_broadcast_network_id, 
			f.broadcast_network_name, 
			a.src_playback_period_cd, 
			a.src_demographic_group, 
			a.src_program_id, 
			a.src_series_name, 
			a.src_program_attributes, 
			a.program_aired_weekday, 
			a.src_episode_id, 
			a.src_episode_number, 
			j.src_episode_title, 
			a.src_daypart_cd, 
			g.src_daypart_name, 
			a.program_telecast_rpt_starttime, 
			a.program_telecast_rpt_endtime, 
			a.src_total_duration, 
			a.avg_audience_proj_000, 
			a.avg_audience_pct, 
			a.avg_audience_pct_nw_cvg_area, 
			a.avg_viewing_hours_units/60 as viewing_hours, 
			a.avg_viewing_hours_persons/60 as viewing_hours_000,
			a.avg_audience_proj_units, 
			a.share_pct, 
			a.share_pct_nw_cvg_area,
			'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_4B' as etl_batch_id, 
			'bi_dbt_user_prd' as etl_insert_user_id, 
			sysdate as etl_insert_rec_dttm, 
			null as etl_update_user_id, 
			cast(null as timestamp) as etl_update_rec_dttm

from
(
--RAW telecasts broken down are to be rolled up as one with start time as min(start time), end time as max(end time) and all the metrics except --VH are rolled up as time-duration based avg - ( metric 1 * duration 1 + metric 2* duration* 2 + metric 3 * duration 3) /(duration 1 + duration --2 + duration 3) here..
select 		broadcast_date_id, 
			broadcast_date, 
			orig_broadcast_date_id, 
			dim_nl_broadcast_network_id, 
			src_broadcast_network_id, 
			src_playback_period_cd, 
			src_demographic_group, 
			src_program_id, 
			src_series_name, 
			dim_source_sys_id,
			src_program_attributes, 
			program_aired_weekday, 
			dim_nl_episode_id, 
			src_episode_id, 
			src_episode_number, 
			dim_nl_daypart_id, 
			src_daypart_cd,
			min(program_telecast_rpt_starttime) as program_telecast_rpt_starttime, 
			max(program_telecast_rpt_endtime) as program_telecast_rpt_endtime,
			sum(src_total_duration) as src_total_duration,
			(sum(avg_audience_proj_000 * src_total_duration)/nullif(sum(nvl2(avg_audience_proj_000, src_total_duration, null)), 0)) as avg_audience_proj_000,
			(sum(avg_audience_pct * src_total_duration)/nullif(sum(nvl2(avg_audience_pct, src_total_duration, null)), 0)) as avg_audience_pct,
			(sum(avg_audience_pct_nw_cvg_area * src_total_duration)/nullif(sum(nvl2(avg_audience_pct_nw_cvg_area, src_total_duration, null)), 0)) as avg_audience_pct_nw_cvg_area,
			sum(avg_viewing_hours_units) as avg_viewing_hours_units, sum(avg_viewing_hours_persons) as avg_viewing_hours_persons,
			(sum(avg_audience_proj_units * src_total_duration)/nullif(sum(nvl2(avg_audience_proj_units, src_total_duration, null)), 0)) as avg_audience_proj_units,
			(sum(share_pct * src_total_duration)/nullif(sum(nvl2(share_pct, src_total_duration, null)), 0)) as share_pct,
			(sum(share_pct_nw_cvg_area * src_total_duration)/nullif(sum(nvl2(share_pct_nw_cvg_area, src_total_duration, null)), 0)) as share_pct_nw_cvg_area

from 		{{source('fds_nl','fact_nl_program_viewership_ratings')}} b

join 		(select dim_nl_series_id, src_series_name from {{source('fds_nl','dim_nl_series')}} where wwe_series_qualifier = 'WWE') c on b.dim_nl_series_id = c.dim_nl_series_id

where 		src_program_id = 296881 and src_program_attributes <> '(R)'

{% if is_incremental() %}
	and b.etl_insert_rec_dttm > coalesce((select max(etl_insert_rec_dttm) from {{this}}), '1900-01-01 00:00:00') 
{% endif %}

group by 	1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17

union

select 		broadcast_date_id, 
			broadcast_date, 
			orig_broadcast_date_id, 
			dim_nl_broadcast_network_id, 
			src_broadcast_network_id, 
			src_playback_period_cd, 
			src_demographic_group, 
			src_program_id, 
			src_series_name, 
			dim_source_sys_id,
			src_program_attributes, 
			program_aired_weekday, 
			dim_nl_episode_id, 
			src_episode_id, 
			src_episode_number, 
			dim_nl_daypart_id, 
			src_daypart_cd, 
			program_telecast_rpt_starttime, 
			program_telecast_rpt_endtime,
			src_total_duration, 
			avg_audience_proj_000, 
			avg_audience_pct, 
			avg_audience_pct_nw_cvg_area, 
			avg_viewing_hours_units,
			avg_viewing_hours_persons, 
			avg_audience_proj_units, 
			share_pct, 
			share_pct_nw_cvg_area

from 		{{source('fds_nl','fact_nl_program_viewership_ratings')}} b

join 		(select dim_nl_series_id, src_series_name from {{source('fds_nl','dim_nl_series')}} where wwe_series_qualifier = 'WWE') c on b.dim_nl_series_id = c.dim_nl_series_id

where 		src_program_id <> 296881 and src_program_attributes <> '(R)'

{% if is_incremental() %}
	and b.etl_insert_rec_dttm > coalesce((select max(etl_insert_rec_dttm) from {{this}}), '1900-01-01 00:00:00') 
{% endif %}

) a

left join 	{{ source('cdm','dim_date') }} d on a.broadcast_date_id = d.dim_date_id

left join 
(
select 		h.dim_date_id, 
			trunc(financial_year_week_begin_date) as fin_year_week_begin_date, 
			trunc(financial_year_week_end_date) as fin_year_week_end_date, 
			financial_year_week_number, 
			financial_month_number, 
			mth_abbr_nm as financial_month_name, 
			financial_quarter, 
			financial_year
			
from 		{{ source('udl_nl','nielsen_finance_yearly_calendar') }} h

join 		(select distinct cal_mth_num, mth_abbr_nm from {{source('cdm','dim_date')}}) i on h.financial_month_number = i.cal_mth_num

where 		dim_date_id >= 20140101

) e on a.broadcast_date_id = e.dim_date_id

left join 	{{source('fds_nl','dim_nl_broadcast_network')}} f on a.dim_nl_broadcast_network_id = f.dim_nl_broadcast_network_id

left join 	{{source('fds_nl','dim_nl_daypart')}} g on a.dim_nl_daypart_id = g.dim_nl_daypart_id

left join 	{{source('fds_nl','dim_nl_episode')}} j on a.dim_nl_episode_id = j.dim_nl_episode_id and a.dim_source_sys_id = j.dim_source_sys_id