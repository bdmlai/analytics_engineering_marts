{{
  config({
	'schema': 'fds_kntr',"materialized": 'view','tags': "Phase4B","persist_docs": {'relation' : true, 'columns' : true}
	})
}}
select week_start_date, src_country, src_channel, src_property, demographic, hd_flag, 
duration_hours, rat_value, viewing_hours, telecasts_count, weekly_cumulative_audience
from {{ref('aggr_kntr_weekly_competitive_program_ratings')}}
