
select a.src_country, a.src_channel
from __dbt__CTE__intm_kntr_country_channel_vh a 
join __dbt__CTE__intm_kntr_country_vh b on a.src_country = b.src_country and a.broadcast_start_date = b.broadcast_start_date
where a.total_watched_mins/b.country_watched_mins <= 0.01