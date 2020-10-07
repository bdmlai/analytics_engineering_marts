
select src_country, src_channel, min(broadcast_date) as broadcast_start_date, sum(watched_mins) as total_watched_mins
from "entdwdb"."fds_kntr"."fact_kntr_wwe_telecast_data"
where demographic_type = 'Everyone'
group by 1, 2
having datediff(d, min(broadcast_date) :: date, current_date) > 365