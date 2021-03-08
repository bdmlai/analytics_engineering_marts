

with __dbt__CTE__intm_nl_weekly_wwe_programs as (


select broadcast_fin_week_begin_date as week,
broadcast_fin_year as year,
case when src_program_id = '296881'  then 'RAW'
     when src_program_id = '898521' or src_program_id = '1000131' or src_program_id = '339681' then 'SmackDown'
     when src_program_id = '436999' then 'NXT' end as program_type,
src_demographic_group,
src_playback_period_cd,
avg(avg_audience_proj_000) as avg_audience_proj_000,
avg(avg_audience_pct) as avg_audience_pct,
count(*) as count_record
from  "entdwdb"."fds_nl"."rpt_nl_daily_wwe_program_ratings"
where src_program_id in ( '436999', '296881', '898521', '1000131', '339681') 
 and  src_program_attributes not like '%(R)%'
group by 1,2,3,4,5
order by 1,2,3,4,5
),  __dbt__CTE__intm_nl_weekly_aew_programs as (


select e.fin_year_week_begin_date as week,
e.financial_year as year,
'AEW'  as program_type,
src_demographic_group,
src_playback_period_cd,
avg(avg_audience_proj_000) as avg_audience_proj_000,
avg(avg_audience_pct) as avg_audience_pct,
count(*) as record_count
from "entdwdb"."fds_nl"."fact_nl_program_viewership_ratings" a 
left join "entdwdb"."cdm"."dim_date" d on a.broadcast_date_id = d.dim_date_id
left join 
(select h.dim_date_id, trunc(financial_year_week_begin_date) as fin_year_week_begin_date, 
trunc(financial_year_week_end_date) as fin_year_week_end_date, financial_year_week_number, financial_month_number, 
mth_abbr_nm as financial_month_name, financial_quarter, financial_year
from "entdwdb"."udl_nl"."nielsen_finance_yearly_calendar" h
join (select distinct cal_mth_num, mth_abbr_nm from "entdwdb"."cdm"."dim_date") i on h.financial_month_number = i.cal_mth_num
where dim_date_id >= 20140101) e on a.broadcast_date_id = e.dim_date_id
where src_program_id in ('459734') 
and src_program_attributes not like '%(R)%'
group by 1,2,3,4,5
order by 1,2,3,4,5
)select week,year,program_type,src_demographic_group,src_playback_period_cd,
 avg_audience_proj_000, avg_audience_pct,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_content' as etl_batch_id,
 'bi_dbt_user_prd' as etl_insert_user_id, 
current_timestamp as etl_insert_rec_dttm, 
null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from
(
select week,year,program_type,src_demographic_group,
       src_playback_period_cd, avg_audience_proj_000, avg_audience_pct 
from __dbt__CTE__intm_nl_weekly_wwe_programs

union all
select week,year,program_type,src_demographic_group,
       src_playback_period_cd, avg_audience_proj_000, avg_audience_pct  
from  __dbt__CTE__intm_nl_weekly_aew_programs
)