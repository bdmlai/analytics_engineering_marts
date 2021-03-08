

 select  a.coverage_area,a.src_market_break,a.src_demographic_group,
a.broadcast_Date,a.src_broadcast_network_name,a.time_minute,absolute_network_number,
 round((a.absolute_network_number/nullif(a.most_current_us_audience_avg_proj_000,0))*100 ,5)
 as stay_percent from __dbt__CTE__intermediate_nl_switching_absolute_network_num a
 where  (src_broadcast_network_name,switching_behavior_dist_cd)
in (('USA','usa'),('FOX Affiliates','fox_affiliates'),('TNT','tnt'))