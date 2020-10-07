
select dim_date_id, broadcast_date, src_weekday, month_num, src_country, broadcast_network_prem_type, src_channel, 
'Income' as demographic_type, 'High Income' as demographic_group_name, src_series, series_episode_name, series_episode_num,
series_name, series_type, start_time, end_time, duration_mins, hd_flag, week_start_date, 
(everyone_rat_value - (medium_rat_value + low_rat_value))  as rat_value, 
(everyone_watched_mins - (medium_watched_mins + low_watched_mins))  as watched_mins,
(everyone_aud - (medium_aud + low_aud))  as aud
from
(select dim_date_id, broadcast_date, src_weekday, month_num, src_country, broadcast_network_prem_type, src_channel, src_series, 
series_episode_name, series_episode_num, series_name, series_type, start_time, end_time, duration_mins, hd_flag, week_start_date,
sum(case 
	when demographic_group_name = 'Everyone' then rat_value 
	else null end) as everyone_rat_value,
sum(case 
	when demographic_group_name = 'Everyone' then watched_mins 
	else null end) as everyone_watched_mins,
sum(case 
	when demographic_group_name = 'Everyone' then aud 
	else null end) as everyone_aud,
sum(case 
	when demographic_group_name = 'Medium Income' then rat_value 
	else null end) as medium_rat_value,
sum(case 
	when demographic_group_name = 'Medium Income' then watched_mins 
	else null end) as medium_watched_mins,
sum(case 
	when demographic_group_name = 'Medium Income' then aud 
	else null end) as medium_aud,
sum(case 
	when demographic_group_name = 'Low Income' then rat_value 
	else null end) as low_rat_value,
sum(case 
	when demographic_group_name = 'Low Income' then watched_mins 
	else null end) as low_watched_mins,
sum(case 
	when demographic_group_name = 'Low Income' then aud 
	else null end) as low_aud
from "entdwdb"."fds_kntr"."fact_kntr_wwe_telecast_data"
where src_country = 'germany' and
demographic_group_name in ('Everyone' , 'Medium Income', 'Low Income')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17 ) a
where (a.everyone_rat_value is not null and a.medium_rat_value is not null and a.low_rat_value is not null) or
(a.everyone_watched_mins is not null and a.medium_watched_mins is not null and a.low_watched_mins is not null) or
(a.everyone_aud is not null and a.medium_aud is not null and a.low_aud is not null)