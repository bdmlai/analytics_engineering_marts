{{
  config({
	'schema': 'fds_kntr',"materialized": 'view','tags': "Phase4B","persist_docs": {'relation' : true, 'columns' : true}
	})
}}
select dim_date_id, broadcast_date, src_weekday, month_name, month_num, modified_month, year, src_country, region, 
broadcast_network_prem_type, src_channel, demographic_type, demographic_group_name, src_series, series_episode_name,
series_episode_num, series_name, series_type, start_time, end_time, duration_mins, hd_flag, start_time_modified,
channel_1, program_1, rat_value, viewing_hours, regional_viewing_hours
from {{ref('rpt_kntr_schedule_vh_data')}}