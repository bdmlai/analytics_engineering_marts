{{
  config({
	'schema': 'fds_kntr',"materialized": 'view','tags': "Phase4B","persist_docs": {'relation' : true, 'columns' : true}
	})
}}
select quarter, year, src_country, src_channel, src_property, demographic, hd_flag, sum(duration_hours) as duration_hours,
(sum(rat_value * total_duration_mins))/(nullif(sum(nvl2(rat_value, total_duration_mins, null)),0)) as rat_value,
sum(viewing_hours) as viewing_hours, sum(telecasts_count) as telecasts_count,
avg(weekly_cumulative_audience) as average_weekly_cumulative_audience_000
from {{ref('aggr_kntr_weekly_competitive_program_ratings')}}
group by 1,2,3,4,5,6,7