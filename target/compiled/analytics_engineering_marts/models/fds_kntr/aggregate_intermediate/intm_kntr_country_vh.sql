
with __dbt__CTE__intm_kntr_country_channel_vh as (

select src_country, src_channel, min(broadcast_date) as broadcast_start_date, sum(watched_mins) as total_watched_mins
from "entdwdb"."fds_kntr"."fact_kntr_wwe_telecast_data"
where demographic_type = 'Everyone'
group by 1, 2
having datediff(d, min(broadcast_date) :: date, current_date) > 365
)select a.src_country, b.broadcast_start_date,  sum(a.watched_mins) as country_watched_mins
from "entdwdb"."fds_kntr"."fact_kntr_wwe_telecast_data" a
join (select distinct src_country, broadcast_start_date   from __dbt__CTE__intm_kntr_country_channel_vh) b on a.src_country = b.src_country
where a.demographic_type = 'Everyone' and a.broadcast_date >= b.broadcast_start_date
group by 1, 2