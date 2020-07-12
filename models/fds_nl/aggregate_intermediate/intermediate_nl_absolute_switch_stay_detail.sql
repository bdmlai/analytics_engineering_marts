
{{
  config({
		"materialized": 'ephemeral'
  })
}}

select a.coverage_area,a.src_market_break,a.src_demographic_group,
a.broadcast_Date,a.src_broadcast_network_name,a.time_minute,
a.most_current_us_audience_avg_proj_000,
a.absolute_network_number,
c.absolute_network_number as absolute_set_off_off_air,
b.absolute_network_number as absolute_stay,
 b.stay_percent , 
 (a.absolute_network_number-b.absolute_network_number-c.absolute_network_number) as absolute_switch ,
 round((((a.absolute_network_number-b.absolute_network_number)-c.absolute_network_number)/nullif(a.most_current_us_audience_avg_proj_000,0))*100,5) as switch_percent
FROM {{ref('intermediate_nl_absolute_network_total_num')}} a  LEFT JOIN {{ref('intermediate_nl_absolute_usa_fox_tnt_stay')}}  B
ON a.src_demographic_group = b.src_demographic_group and 
a.broadcast_Date = b.broadcast_Date and
a.src_broadcast_network_name = b.src_broadcast_network_name and
a.time_minute = b. time_minute
left join {{ref('intermediate_nl_set_off_air_absolute_network_num')}}   c
ON a.src_demographic_group = c.src_demographic_group and 
a.broadcast_Date = c.broadcast_Date and
a.src_broadcast_network_name = c.src_broadcast_network_name and
a.time_minute = c. time_minute