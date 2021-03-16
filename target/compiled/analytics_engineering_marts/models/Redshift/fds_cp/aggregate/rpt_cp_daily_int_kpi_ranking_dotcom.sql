

select *,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_CP' AS etl_batch_id, 
		'bi_dbt_user_prd' as etl_insert_user_id,
		sysdate etl_insert_rec_dttm,
		'' etl_update_user_id,
		sysdate etl_update_rec_dttm 
from (
select 
c.date,
d.region,
c.country,
c.property,
sum(c.unique_visitors) unique_visitors,
avg(c.visits) visits,
avg(c.page_views) page_views,
avg(c.video_views) video_views,
avg(c.hours_watched) hours_watched,
avg(d.Population) population
from
(select 
a.date,
a.country,
a.property,
sum(a.unique_visitors) unique_visitors,
avg(a.visits) visits,
avg(a.page_views) page_views,
avg(a.video_views) video_views,
sum(b.hours_watched) hours_watched
from
(select date, geonetwork_country as country, property,
sum(case when metric_name= 'Total Unique Visitors' then metric_value else 0 end) as unique_visitors,
sum(case when metric_name= 'Visits' then metric_value else 0 end) as visits,
sum(case when metric_name= 'Page Views' then metric_value else 0 end) as page_views,
sum(case when metric_name= 'Video Views' then metric_value else 0 end) as video_views
from "entdwdb"."fds_da"."dm_digital_kpi_datamart_monthly_topline"
where (property='WWE.com') 
and geonetwork_country <>'All' and geonetwork_gm_region_wwe_ref ='All' and device_type='All' AND geonetwork_super_region_wwe_ref='All'
and geonetwork_us_v_international='Global'
  group by 1,2,3
) a 
 full outer join
(select country, to_date(trunc(start_time), 'yyyy-mm-01') as Watch_month,
'WWE.com' as property,
sum(play_time)/3600 as hours_watched
from "entdwdb"."fds_nplus"."vw_fact_daily_dotcom_viewership"
where trunc(start_time) >= to_date('2018-01-01', 'yyyy-mm-01') and trunc(start_time) < to_date(current_date, 'yyyy-mm-01')
group by 2,1,3
order by 2,1,3) b
on a.date=b.watch_month and
lower(a.property)=lower(b.property) and
lower(a.country) = lower(b.country)
where a.date is not null and b.watch_month is not null
group by 1,2,3
order by 1,2,3) c
left join 
(select
e.country,
f.region_nm as region,
avg(e.population) as population
from
(select case when country_name='USA' then 'United States' else country_name end as Country, sum(population) as Population 
from "entdwdb"."cdm"."dim_country_population" group by 1) e
left join
(select distinct country_nm, region_nm from "entdwdb"."cdm"."dim_region_country" where ent_map_nm = 'GM Region') f
on upper(e.country)=upper(f.country_nm)
group by 1,2
) d
on lower(c.country)=lower(d.country)
where c.date is not null and d.population is not null
group by 1,2,3,4
order by 1,2,3,4
)