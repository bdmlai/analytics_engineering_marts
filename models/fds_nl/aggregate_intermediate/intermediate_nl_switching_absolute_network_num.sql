
{{
  config({
		'schema': 'fds_nl',
		"materialized": 'ephemeral'
  })
}}

select a.coverage_area,a.src_market_break,a.src_demographic_group,a.broadcast_date,a.src_broadcast_network_name,
a.switching_behavior_dist_cd,
a.time_minute ,b.most_current_us_audience_avg_proj_000,a.switching_behavior_dist_Cd_Value,
((switching_behavior_dist_Cd_Value/100 ) * most_current_us_audience_avg_proj_000 ) as absolute_network_number 
 from {{source('fds_nl','fact_nl_weekly_live_switching_behavior_destination_dist')}}  a
join  {{ref('intermediate_nl_minxmin_ratings')}} b
 on (a.src_market_break)= (b.src_market_break)
and (a.src_demographic_group) = (b.src_demographic_group)
 and a.broadcast_date=b.broadcast_date
and (a.src_broadcast_network_name)= (b.src_broadcast_network_name) and 
upper(a.source_name) = upper(b.mxm_source) and
(split_part(a.time_minute, ':', 1) :: int *60*60 +
 split_part(a.time_minute, ':', 2) :: int*60 +
  (split_part(a.time_minute, ':', 3) :: int)) = b.time_minute
 where a.etl_insert_rec_dttm  >  coalesce ((select max(etl_insert_rec_dttm) from
  fds_nl.rpt_nl_weekly_channel_switch), '1900-01-01 00:00:00')
or b.etl_insert_rec_dttm  >  coalesce ((select max(etl_insert_rec_dttm) from
  fds_nl.rpt_nl_weekly_channel_switch), '1900-01-01 00:00:00')


  
