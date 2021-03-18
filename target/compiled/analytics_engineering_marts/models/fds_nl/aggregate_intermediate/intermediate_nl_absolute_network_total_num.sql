

select a.broadcast_Date,a.coverage_area,a.src_market_break,a.src_broadcast_network_name,a.src_demographic_group,
a.time_minute,a.source_name,
most_current_us_audience_avg_proj_000,
sum(absolute_network_number) as absolute_network_number
from __dbt__CTE__intermediate_nl_switching_absolute_network_num  a 
group by 1,2,3,4,5,6,7,8