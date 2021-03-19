{{
  config({
		"materialized": 'ephemeral'
  })
}}
select a.src_country, b.broadcast_start_date,  sum(a.watched_mins) as country_watched_mins
from {{source('fds_kntr','fact_kntr_wwe_telecast_data')}} a
join (select distinct src_country, broadcast_start_date   from {{ref('intm_kntr_country_channel_vh')}}) b on a.src_country = b.src_country
where a.demographic_type = 'Everyone' and a.broadcast_date >= b.broadcast_start_date
group by 1, 2