

  create  table
    "entdwdb"."fds_kntr"."aggr_kntr_weekly_competitive_program_ratings__dbt_tmp"
    
    
  as (
    
select c.*, 'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_4B' as etl_batch_id,
'bi_dbt_user_prd' as etl_insert_user_id, current_timestamp as etl_insert_rec_dttm, 
null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from
(select cal_year_week_begin_date as week_start_date, cal_year_week_end_date as week_end_date, 
cal_year_week_num as week_num, to_char(cal_year_week_begin_date, 'mon') as month,
('q' || extract(quarter from cal_year_week_begin_date)) as quarter, 
extract(yr from cal_year_week_begin_date) as year,
src_country, src_channel, src_property, demographic, 
case
	when lower(src_channel) like '%hd%' then 'Yes'
	else 'No' end as hd_flag,
sum(length_avg_tm) as total_duration_mins, sum(length_avg_tm/60) as duration_hours,
(sum(rat_num_avg_wg * length_avg_tm))/(nullif(sum(nvl2(rat_num_avg_wg, length_avg_tm, null)),0)) as rat_value,
sum((rat_num_avg_wg * 1000 * length_avg_tm) / 60) as viewing_hours,
count(*) as telecasts_count,
sum(rat_num_avg_wg * 1000) as weekly_cumulative_audience
from "entdwdb"."fds_kntr"."fact_kntr_annual_profile" a
join "entdwdb"."cdm"."dim_date" b on a.broadcast_date_id = b.dim_date_id
group by 1,2,3,4,5,6,7,8,9,10,11 ) c
  );