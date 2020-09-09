{{
  config({
		"materialized": 'ephemeral'
  })
}}
select a.dim_date_id, a.broadcast_date, a.src_weekday, to_char(a.broadcast_date :: date, 'mon') as month_name, a.month_num,
(substring(trim(broadcast_date), 1, 8) || '01') as modified_month, extract(yr from broadcast_date :: date) as year,
case 
	when upper(a.src_country)='UNITED ARAB EMIRATES' then 'APAC except India' 
	else b.region end as region,
a.src_country, a.broadcast_network_prem_type, a.src_channel, a.demographic_type, 
a.demographic_group_name, a.src_series, a.series_episode_name, a.series_episode_num,
a.series_name, a.series_type, a.start_time, a.end_time, a.duration_mins, a.hd_flag, a.week_start_date,
(substring((dateadd(m, 30, ((a.broadcast_date || ' ' || a.start_time) :: timestamp))), 1, 14) || '00:00') as start_time_modified,
nvl2(c.src_channel, 'Others', a.src_channel) as channel_1,
case 
	when upper(a.series_name) in ('RAW','SMACKDOWN','NXT','PPV','SUNDAY DHAMAAL','SATURDAY NIGHT','TOTAL BELLAS','TOTAL DIVAS') then a.series_name 
	else 'Others' end as program_1, 
(sum(a.rat_value*a.duration_mins))/(nullif(sum(nvl2(a.rat_value,a.duration_mins,null)),0)) as rat_value,
sum(a.watched_mins/60) as viewing_hours, sum(aud) as aud
from 
(select dim_date_id, broadcast_date, src_weekday, month_num, src_country, broadcast_network_prem_type, src_channel, 
demographic_type, demographic_group_name, src_series, series_episode_name, series_episode_num,
series_name, series_type, start_time, end_time, duration_mins, hd_flag, week_start_date, rat_value, watched_mins, aud
from {{source('fds_kntr','fact_kntr_wwe_telecast_data')}}
where demographic_type is not null and demographic_group_name is not null
union
select dim_date_id, broadcast_date, src_weekday, month_num, src_country, broadcast_network_prem_type, src_channel, 
demographic_type, demographic_group_name, src_series, series_episode_name, series_episode_num,
series_name, series_type, start_time, end_time, duration_mins, hd_flag, week_start_date, rat_value, watched_mins, aud
from {{ref('intm_kntr_germany_high_income')}}) a
left join {{source('fds_kntr','kantar_static_country_region_tag')}}  b on upper(a.src_country)= upper(b.country)
left join {{ref('intm_kntr_channel_1')}} c on a.src_country = c.src_country and a.src_channel = c.src_channel
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26