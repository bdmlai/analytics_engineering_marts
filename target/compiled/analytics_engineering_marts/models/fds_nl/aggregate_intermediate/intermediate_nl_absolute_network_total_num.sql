

with __dbt__CTE__intermediate_nl_minxmin_ratings as (


select  src_market_break,src_demographic_group,broadcast_date,mxm_source,
case when upper(src_broadcast_network_name)= 'FOX' then 'FOX Affiliates' 
when upper(src_broadcast_network_name) = 'TURNER NETWORK TELEVISION' then 'TNT'
WHEN  upper(src_broadcast_network_name) = 'USA NETWORK' THEN 'USA' END AS src_broadcast_network_name,
((split_part(program_telecast_rpt_starttime, ':', 1) :: int *60*60 +
 split_part(program_telecast_rpt_starttime, ':', 2) :: int*60 +
  (split_part(program_telecast_rpt_starttime, ':', 3) :: int  ))
 + (min_of_pgm_value - 1)*60) AS TIME_MINUTE ,
most_current_us_audience_avg_proj_000,etl_insert_rec_dttm
FROM  "entdwdb"."fds_nl"."fact_nl_minxmin_ratings" 
where src_playback_period_cd in ('Live | TV with Digital | Linear with VOD')
),  __dbt__CTE__intermediate_nl_switching_absolute_network_num as (


select a.coverage_area,a.src_market_break,a.src_demographic_group,a.broadcast_date,a.src_broadcast_network_name,
a.switching_behavior_dist_cd,
a.time_minute ,b.most_current_us_audience_avg_proj_000,a.switching_behavior_dist_Cd_Value,
((switching_behavior_dist_Cd_Value/100 ) * most_current_us_audience_avg_proj_000 ) as absolute_network_number 
 from "entdwdb"."fds_nl"."fact_nl_weekly_live_switching_behavior_destination_dist"  a
join  __dbt__CTE__intermediate_nl_minxmin_ratings b
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
)select a.broadcast_Date,a.coverage_area,a.src_market_break,a.src_broadcast_network_name,a.src_demographic_group,
a.time_minute,
most_current_us_audience_avg_proj_000,
sum(absolute_network_number) as absolute_network_number
from __dbt__CTE__intermediate_nl_switching_absolute_network_num  a 
group by 1,2,3,4,5,6,7