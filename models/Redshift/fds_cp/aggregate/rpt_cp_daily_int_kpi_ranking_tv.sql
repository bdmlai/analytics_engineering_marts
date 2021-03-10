{{
  config({
		'schema': 'fds_cp',
		"pre-hook": "truncate fds_cp.rpt_cp_daily_int_kpi_ranking_tv",
		"materialized": 'incremental','tags': "Content","persist_docs": {'relation' : true, 'columns' : true}
  })
}}


select *,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_CP' AS etl_batch_id, 
		'bi_dbt_user_prd' as etl_insert_user_id,
		sysdate etl_insert_rec_dttm,
		'' etl_update_user_id,
		sysdate etl_update_rec_dttm 
from (
select 
c.week,
c.date,
c.country,
d.region,
c.program,
c.series_type,
sum(c.telecasts) as telecasts,
sum(c.telecast_hours) as telecast_hours,
sum(c.weekly_aud) as weekly_aud,
avg(c.duration_mins) as duration_mins,
avg(d.population) as population
from(select 
a.week,
a.month as date,
a.country,
a.program,
a.series_type,
sum(a.telecasts_count) as telecasts,
sum(b.telecast_hours) as telecast_hours,
sum(a.weekly_aud) as weekly_aud,
avg(b.duration_mins) as duration_mins,
avg(b.aud) as aud
from
(select 
week_start_date as week,
date_trunc('month',week_start_date) as month,
initcap(src_country) as country,
program_1 as program,
--Added column series_type  as part of KPI ranking tv enhancement . Jira - PSTA-1897
series_type,
sum(weekly_cumulative_audience) as weekly_aud,
sum(telecasts_count) as telecasts_count
from {{source('fds_kntr','vw_aggr_kntr_schedule_wca_data')}}
where lower(demographic_type)='everyone' and
extract(year from week_start_date)>extract(year from dateadd(year,-3,getdate()))
and  week_start_date < substring(date_trunc('month',getdate()),1,10)
group by 1,2,3,4,5
order by 1,2,3,4,5
) a 
left join
(select 
week_start_date as week,
date_trunc('month',week_start_date) as month,
initcap(src_country) as country,
program_1 as program,
--Added column series_type and telecast_hours as part of KPI ranking tv enhancement . Jira - PSTA-1897
series_type,
avg(duration_mins) as duration_mins,
sum(aud) as aud,
round(sum(duration_mins/60 :: decimal),2) as telecast_hours
from {{source('fds_kntr','rpt_kntr_schedule_vh_data')}}
where lower(demographic_type)='everyone' and
extract(year from week_start_date)>extract(year from dateadd(year,-3,getdate()))
and  week_start_date < substring(date_trunc('month',getdate()),1,10)
group by 1,2,3,4,5
order by 1,2,3,4,5) b 
on lower(a.country)=lower(b.country) and
lower(a.program)=lower(b.program) and
a.week=b.week
group by 1,2,3,4,5
order by 1,2,3,4,5
)c
left  join 
(select country, region, avg(population) as population from 
(select case when country_name='USA' then 'United States' else country_name end as Country, sum(population) as Population 
from {{source('cdm','dim_country_population')}} group by 1) e
left join
(select distinct country_nm, region_nm as region from {{source('cdm','dim_region_country')}} where ent_map_nm = 'GM Region') f
on upper(e.country)=upper(f.country_nm)
group by 1,2
)d
on lower(c.country)=lower(d.country)
group by 1,2,3,4,5,6
order by 1,2,3,4,5,6
)