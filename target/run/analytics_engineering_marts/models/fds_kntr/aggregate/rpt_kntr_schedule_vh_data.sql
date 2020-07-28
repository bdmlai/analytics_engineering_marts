

  create  table
    "entdwdb"."fds_kntr"."rpt_kntr_schedule_vh_data__dbt_tmp"
    
    
  as (
    
with __dbt__CTE__intm_kntr_germany_high_income as (

select dim_date_id, broadcast_date, src_country, broadcast_network_prem_type, src_channel, 
'Income' as demographic_type, 'High Income' as demographic_group_name,
series_name, series_type, start_time, end_time, duration_mins, hd_flag, week_start_date, 
(everyone_rat_value - (medium_rat_value + low_rat_value))  as rat_value, 
(everyone_watched_mins - (medium_watched_mins + low_watched_mins))  as watched_mins,
(everyone_aud - (medium_aud + low_aud))  as aud
from
(select dim_date_id, broadcast_date, src_country, broadcast_network_prem_type, src_channel, series_name, series_type, start_time, end_time, duration_mins, hd_flag, week_start_date,
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
group by 1,2,3,4,5,6,7,8,9,10,11,12 ) a
where (a.everyone_rat_value is not null and a.medium_rat_value is not null and a.low_rat_value is not null) or
(a.everyone_watched_mins is not null and a.medium_watched_mins is not null and a.low_watched_mins is not null) or
(a.everyone_aud is not null and a.medium_aud is not null and a.low_aud is not null)
),  __dbt__CTE__intm_kntr_country_channel_vh as (

select src_country, src_channel, min(broadcast_date) as broadcast_start_date, sum(watched_mins) as total_watched_mins
from "entdwdb"."fds_kntr"."fact_kntr_wwe_telecast_data"
where demographic_type = 'Everyone'
group by 1, 2
having datediff(d, min(broadcast_date) :: date, current_date) > 365
),  __dbt__CTE__intm_kntr_country_vh as (

select a.src_country, b.broadcast_start_date,  sum(a.watched_mins) as country_watched_mins
from "entdwdb"."fds_kntr"."fact_kntr_wwe_telecast_data" a
join (select distinct src_country, broadcast_start_date   from __dbt__CTE__intm_kntr_country_channel_vh) b on a.src_country = b.src_country
where a.demographic_type = 'Everyone' and a.broadcast_date >= b.broadcast_start_date
group by 1, 2
),  __dbt__CTE__intm_kntr_channel_1 as (

select a.src_country, a.src_channel
from __dbt__CTE__intm_kntr_country_channel_vh a 
join __dbt__CTE__intm_kntr_country_vh b on a.src_country = b.src_country and a.broadcast_start_date = b.broadcast_start_date
where a.total_watched_mins/b.country_watched_mins <= 0.01
),  __dbt__CTE__intm_kntr_schedule_vh_data as (

select a.dim_date_id, a.broadcast_date, to_char(a.broadcast_date :: date, 'mon') as broadcast_month, 
extract(yr from broadcast_date :: date) as broadcast_year,
case 
	when upper(a.src_country)='UNITED ARAB EMIRATES' then 'APAC except India' 
	else b.region end as region,
a.src_country, a.broadcast_network_prem_type, a.src_channel, a.demographic_type, 
a.demographic_group_name, a.series_name, a.series_type, a.start_time, a.end_time, a.duration_mins, a.hd_flag, a.week_start_date,
(substring((dateadd(m, 30, ((a.broadcast_date || ' ' || a.start_time) :: timestamp))), 1, 14) || '00:00') as start_time_modified,
nvl2(c.src_channel, 'Others', a.src_channel) as channel_1,
case 
	when upper(a.series_name) in ('RAW','SMACKDOWN','NXT','PPV','SUNDAY DHAMAAL','SATURDAY NIGHT','TOTAL BELLAS','TOTAL DIVAS') then a.series_name 
	else 'Others' end as program_1, 
(sum(a.rat_value*a.duration_mins))/(nullif(sum(nvl2(a.rat_value,a.duration_mins,null)),0)) as rat_value,
sum(a.watched_mins/60) as viewing_hours, sum(aud) as aud
from 
(select dim_date_id, broadcast_date, src_country, broadcast_network_prem_type, src_channel, demographic_type, demographic_group_name,
series_name, series_type, start_time, end_time, duration_mins, hd_flag, week_start_date, rat_value, watched_mins, aud
from "entdwdb"."fds_kntr"."fact_kntr_wwe_telecast_data"
where demographic_type is not null and demographic_group_name is not null
union
select dim_date_id, broadcast_date, src_country, broadcast_network_prem_type, src_channel, demographic_type, demographic_group_name,
series_name, series_type, start_time, end_time, duration_mins, hd_flag, week_start_date, rat_value, watched_mins, aud
from __dbt__CTE__intm_kntr_germany_high_income) a
left join "entdwdb"."fds_kntr"."kantar_static_country_region_tag"  b on upper(a.src_country)= upper(b.country)
left join __dbt__CTE__intm_kntr_channel_1 c on a.src_country = c.src_country and a.src_channel = c.src_channel
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
),  __dbt__CTE__intm_kntr_region_vh as (

select broadcast_month, broadcast_year, region, demographic_type, demographic_group_name, sum(viewing_hours) as regional_viewing_hours
from __dbt__CTE__intm_kntr_schedule_vh_data
group by 1,2,3,4,5
)select a.*, b.regional_viewing_hours,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_4B' as etl_batch_id,'bi_dbt_user_prd' as etl_insert_user_id, 
current_timestamp as etl_insert_rec_dttm, null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from __dbt__CTE__intm_kntr_schedule_vh_data a
left join __dbt__CTE__intm_kntr_region_vh b on a.broadcast_month = b.broadcast_month and a.broadcast_year = b.broadcast_year
and a.region = b.region and a.demographic_type = b.demographic_type and a.demographic_group_name = b.demographic_group_name
  );