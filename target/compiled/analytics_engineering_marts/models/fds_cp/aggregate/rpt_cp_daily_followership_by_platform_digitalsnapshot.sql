
with __dbt__CTE__intm_cp_monthly_socialmedia_fb as (



select trunc(add_months(to_date(as_on_date,'YYYYMMDD'), -1)) as month,src_country_name as country,
'facebook_followers' as metric,
'NA' as page,
total as values,'Social Media' as platform
from
(
select a.as_on_date,c.src_country_name, sum(a.c_followers) as total 
from "entdwdb"."fds_fbk"."fact_fb_smfollowership_audience_bycountry" as a
left join "entdwdb"."cdm"."dim_country" as c on a.dim_country_id=c.dim_country_id
where a.as_on_date like '%01' 
group by 1,2
)
union all
-- adding facebook local page followers metric ---
select trunc(add_months(to_date(as_on_date,'YYYYMMDD'), -1)) as month, country,
'facebook_local_page_followers' as metric, page,
total as values,'Social Media' as platform
from
(
select a.as_on_date,c.country, page, sum(a.c_followers) as total 
from "entdwdb"."fds_fbk"."fact_fb_smfollowership_audience_bycountry" as a
left join 
(select dim_smprovider_account_id,channel_handle_name as page, as_on_date, country_market as country 
from "entdwdb"."hive_udl_cp"."daily_social_account_country_mapping"
where platform = 'Facebook') as c 
on a.dim_smprovider_account_id=c.dim_smprovider_account_id and a.as_on_date = c.as_on_date
where a.as_on_date like '%01' 
group by 1,2,3
) where country is not null
),  __dbt__CTE__intm_cp_monthly_socialmedia_igm as (


select trunc(add_months(to_date(as_on_date,'YYYYMMDD'), -1)) as month,src_country_name as country,
'igm_followers' as metric,
'NA' as page,
total as values,'Social Media' as platform
from
(
select a.as_on_date,c.src_country_name, sum(a.c_followers) as total 
from "entdwdb"."fds_igm"."fact_ig_smfollowership_audience_bycountry" as a
left join "entdwdb"."cdm"."dim_country" as c on a.dim_country_id=c.dim_country_id
where a.as_on_date like '%01' 
group by 1,2
)

union all
-- adding instagram local page followers
select trunc(add_months(to_date(as_on_date,'YYYYMMDD'), -1)) as month, country,
'instagram_local_page_followers' as metric, page,
total as values,'Social Media' as platform
from
(
select a.as_on_date,c.country, page, sum(a.c_followers) as total 
from "entdwdb"."fds_igm"."fact_ig_smfollowership_audience_bycountry" as a
left join 
(select dim_smprovider_account_id,channel_handle_name as page, as_on_date, country_market as country 
from "entdwdb"."hive_udl_cp"."daily_social_account_country_mapping"
where platform = 'Instagram') as c 
on a.dim_smprovider_account_id=c.dim_smprovider_account_id and a.as_on_date = c.as_on_date
where a.as_on_date like '%01' 
group by 1,2,3
) where country is not null
),  __dbt__CTE__intm_cp_monthly_socialmedia_yt as (


select 
substring(date_trunc('month', to_date(report_date,'YYYYMMDD')),1,10) as month, 
country,
'yt_net_subscribers' as metric,
'NA' as page,
netsubs as values,'Social Media' as platform
from
(select a.report_date,a.netsubs, b.country_nm as country from 
(
(
with _data as (
select report_date,
country_code,
sum(subscribers_gained) as subscribers_gained,
sum(subscribers_lost) as subscribers_lost
from "entdwdb"."fds_yt"."rpt_yt_wwe_engagement_daily"
group by 1,2
)
select
  report_date,
  country_code,subscribers_gained, subscribers_lost,
  sum(subscribers_gained) over (partition by country_code order by report_date asc rows between unbounded preceding and current row) as cumulative_gained,
  sum(subscribers_lost) over (partition by country_code order by report_date asc rows between unbounded preceding and current row) as cumulative_lost,
(cumulative_gained - cumulative_lost) as netsubs 
from _data
) as a
left join
(select iso_alpha2_ctry_cd, country_nm from "entdwdb"."cdm"."dim_region_country" where ent_map_nm = 'GM Region'
) as b
on upper(a.country_code) = upper(b.iso_alpha2_ctry_cd)
)
where to_date(report_date,'YYYYMMDD') = last_day(to_date(report_date,'YYYYMMDD'))
) where country is not null
),  __dbt__CTE__intm_cp_monthly_youtube as (

   
select to_date(report_date,'YYYYMMDD') as report_date,
country_name2 as country,type, 
sum(case when type='UGC' then views end ) as UGC_views,
sum(case when type='Owned' then views end) as owned_views,
sum(case when type='UGC' then hours_watched end) as UGC_hours_watched,
sum(case when type='Owned' then hours_watched end) as owned_hours_watched
 from "entdwdb"."fds_yt"."agg_yt_monetization_summary"
 where report_date >= '20170101'  and country_name2 is not null
 group by report_date,country_name2,type
),  __dbt__CTE__intm_cp_monthly_youtube_combined as (

 
  select trunc(date_trunc('month',report_date)) as month,
        country,'UGC Views' as metric,'NA' as page ,
        round(sum(coalesce(ugc_views,0))) as values,
        'YouTube' as platform      
 from  __dbt__CTE__intm_cp_monthly_youtube
 group by month,country,metric
 
 union all
 select trunc(date_trunc('month',report_date)) as month,
        country,'Owned Views' as metric,'NA' as page,
        round(sum(coalesce(owned_views,0))) as values,
        'YouTube' as platform       
 from   __dbt__CTE__intm_cp_monthly_youtube
 group by month,country,metric
 
 union all
 select trunc(date_trunc('month',report_date)) as month,
        country,'UGC Hours' as metric,'NA' as page,
        round(sum(coalesce(ugc_hours_watched,0))) as values,
        'YouTube' as platform 
 from   __dbt__CTE__intm_cp_monthly_youtube
 group by month,country,metric
 
 union all
 select trunc(date_trunc('month',report_date)) as month,
        country,'Owned Hours' as metric,'NA'as page,
        round(sum(coalesce(owned_hours_watched,0))) as values,
        'YouTube' as platform       
 from   __dbt__CTE__intm_cp_monthly_youtube
 group by month,country,metric
 
 union all
 select trunc(date_trunc('month',report_date)) as month,
        country,'Owned+UGC Views' as metric,'NA'as page,
        abs(round(sum(coalesce(owned_views,0)))+round(sum(coalesce(ugc_views,0)))) as values,
        'YouTube' as platform       
 from   __dbt__CTE__intm_cp_monthly_youtube
 group by month,country,metric
 
 union all
 select trunc(date_trunc('month',report_date)) as month,
        country,'Owned+UGC Hours' as metric,'NA'as page,
       abs(round(sum(coalesce(owned_hours_watched,0)))+round(sum(coalesce(ugc_hours_watched,0)))) as values,
        'YouTube' as platform       
 from   __dbt__CTE__intm_cp_monthly_youtube
 group by month,country,metric
),  __dbt__CTE__intm_cp_monthly_dotcom as (


select 
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
where (property='WWE.com' ) 
and geonetwork_country <>'All' and geonetwork_gm_region_wwe_ref='All' AND geonetwork_super_region_wwe_ref='All'
and device_type='All' and geonetwork_us_v_international='Global'
and date<(select date_trunc('month',max(date)) from "entdwdb"."fds_da"."dm_digital_kpi_datamart_monthly_topline")  group by 1,2,3
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
),  __dbt__CTE__intm_cp_monthly_dotcom_combined as (


select date as month,country,
'.COM Page Views' as metric,
'NA' as page,
page_views as values,'.COM' as platform
from __dbt__CTE__intm_cp_monthly_dotcom

union all

select date as month,country,
'.COM Views' as metric,
'NA' as page,
video_views as values,'.COM' as platform
from __dbt__CTE__intm_cp_monthly_dotcom

union all

select date as month,country,
'.COM Hours' as metric,
'NA' as page,
hours_watched as values,'.COM' as platform
from __dbt__CTE__intm_cp_monthly_dotcom

union all

select date as month,country,
'.COM Monthly Total Unique Cookies' as metric,
'NA' as page,
unique_visitors as values,'.COM' as platform
from __dbt__CTE__intm_cp_monthly_dotcom

union all

select date as month,country,
'.COM Visits' as metric,
'NA' as page,
visits as values,'.COM' as platform
from __dbt__CTE__intm_cp_monthly_dotcom
)select month,country country_name,metric content_type,page,values as followers_count,platform,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_CP' AS etl_batch_id,
'bi_dbt_user_prd' as etl_insert_user_id,
sysdate etl_insert_rec_dttm,
'' etl_update_user_id,
sysdate etl_update_rec_dttm   from
(
select month,initcap(country) as country,metric,page,values,platform from __dbt__CTE__intm_cp_monthly_socialmedia_fb
union all 
select month,initcap(country) as country,metric,page,values,platform from __dbt__CTE__intm_cp_monthly_socialmedia_igm
union all
select cast(month as date),country,metric,page,cast(round(values) as int) as values,platform from __dbt__CTE__intm_cp_monthly_socialmedia_yt
union all
select * from __dbt__CTE__intm_cp_monthly_youtube_combined
union all
select * from __dbt__CTE__intm_cp_monthly_dotcom_combined
)