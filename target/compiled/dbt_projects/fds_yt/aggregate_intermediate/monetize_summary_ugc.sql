
with __dbt__CTE__rpt_yt_ugc_engagement_daily as (



select * from fds_yt.rpt_yt_ugc_engagement_daily
),  __dbt__CTE__dim_region_country as (


select * from cdm.dim_region_country
),  __dbt__CTE__ugc_engage_measures as (



select report_date,report_date_dt,country_code,
case when country_code='US' then 'United States'
     when country_code='IN' then 'India'
     when country_code='GB' then 'United Kingdom'
     else 'ROW' end as country_name,
d.region_nm as region2,initcap(d.country_nm) as country_name2,
sum(views) views,sum(watch_time_minutes)/60 as hours_watched,
sum(watch_time_minutes) as watch_time_minutes,
sum(likes) likes,
sum(dislikes) dislikes,
sum(subscribers_gained) subscribers_gained, 
sum(subscribers_lost) subscribers_lost
from __dbt__CTE__rpt_yt_ugc_engagement_daily ue
left join
(select distinct upper(iso_alpha2_ctry_cd) as iso_alpha2_ctry_cd ,country_nm,region_nm from __dbt__CTE__dim_region_country
where etl_source_name='Youtube') d
on ue.country_code=d.iso_alpha2_ctry_cd
where report_date_dt between current_date - 52 and current_date - 1 
and dim_source_type_id in (2,3)
group by 1,2,3,4,5,6
),  __dbt__CTE__rpt_yt_revenue_daily as (



select * from fds_yt.rpt_yt_revenue_daily
),  __dbt__CTE__ugc_revenue_views_uploader_thirdparty as (



select r.report_date,r.country_code,
case when r.country_code='US' then 'United States'
     when r.country_code='IN' then 'India'
     when r.country_code='GB' then 'United Kingdom'
else 'ROW' end as country_name, 
sum(r.ad_impressions) ad_impressions, sum(r.estimated_youtube_ad_revenue) estimated_youtube_ad_revenue, 
sum(r.estimated_partner_revenue) estimated_partner_revenue,
sum(case when r.video_id+r.report_date in 
(select distinct video_id+report_date from __dbt__CTE__rpt_yt_revenue_daily where estimated_partner_revenue!=0 group by video_id, report_date)
then views else 0 end) revenue_views
from 
(select report_date,country_code,ad_impressions,estimated_youtube_ad_revenue,estimated_partner_revenue,video_id,views,uploader_type
from __dbt__CTE__rpt_yt_revenue_daily where report_date between to_char(current_date - 52, 'YYYYMMDD')   and to_char(current_date - 1, 'YYYYMMDD') 
and uploader_type in ('self' ,'thirdParty')) r 
where r.report_date between to_char(current_date - 52, 'YYYYMMDD') and to_char(current_date - 1, 'YYYYMMDD')
and r.uploader_type='thirdParty' 
group by 1,2,3
),  __dbt__CTE__rpt_yt_demographics_views_daily as (


select * from fds_yt.rpt_yt_demographics_views_daily
),  __dbt__CTE__ugc_demograph_by_gender_age_group as (



select report_date, country_code,
case when d.country_code='US' then 'United States'
     when d.country_code='IN' then 'India'
     when d.country_code='GB' then 'United Kingdom'
else 'ROW' end as country_name,
coalesce(sum(case when gender='MALE' then views*1000000 else 0 end)/nullif(sum(views),0),0) as male,
coalesce(sum(case when gender='FEMALE' then views*1000000 else 0 end)/nullif(sum(views),0),0) as female,
coalesce(sum(case when gender='GENDER_OTHER' then views*1000000 else 0 end)/nullif(sum(views),0),0) as gender_other,
coalesce(sum(case when age_group='AGE_25_34' then views*1000000 else 0 end)/nullif(sum(views),0),0) as AGE_25_34,
coalesce(sum(case when age_group='AGE_45_54' then views*1000000 else 0 end)/nullif(sum(views),0),0) as AGE_45_54,
coalesce(sum(case when age_group='AGE_13_17' then views*1000000 else 0 end)/nullif(sum(views),0),0) as AGE_13_17,
coalesce(sum(case when age_group='AGE_35_44' then views*1000000 else 0 end)/nullif(sum(views),0),0) as AGE_35_44,
coalesce(sum(case when age_group='AGE_55_64' then views*1000000 else 0 end)/nullif(sum(views),0),0) as AGE_55_64,
coalesce(sum(case when age_group='AGE_65_' then views*1000000 else 0 end)/nullif(sum(views),0),0) as AGE_65_,
coalesce(sum(case when age_group='AGE_18_24' then views*1000000 else 0 end)/nullif(sum(views),0),0) as AGE_18_24
from (select report_date,channel_name,country_code,duration,report_date_dt,time_uploaded,gender,age_group,views,video_id,uploader_type
from __dbt__CTE__rpt_yt_demographics_views_daily where report_date_dt between current_date-52 and current_date - 1 and uploader_type in ( 'self' , 'thirdParty')) d 
where d.report_date_dt between current_date - 52 and current_date - 1  and d.uploader_type = 'thirdParty'
group by 1,2,3
)select a.report_date_dt,a.report_date,'UGC' as channel_name,a.country_name,a.country_code,'' as duration_group,'' as debut_type, '' as owned_class, a.region2, a.country_name2,
sum(a.views) as views,
sum(a.hours_watched) as hours_watched,
sum(a.watch_time_minutes) as watch_time_minutes,
sum(b.ad_impressions) as ad_impressions, 
sum(b.estimated_youtube_ad_revenue) as yt_ad_revenue,
sum(b.estimated_partner_revenue) as partner_revenue,
sum(a.likes) likes,
sum(a.dislikes) dislikes,
sum(a.subscribers_gained) subscribers_gained, 
sum(a.subscribers_lost) subscribers_lost,
sum(b.revenue_views) revenue_views,
sum(c.male) male,
sum(c.female) as female,
sum(c.gender_other) gender_other,
sum(c.AGE_25_34) AGE_25_34,
sum(c.AGE_45_54) AGE_45_54,
sum(c.AGE_13_17) AGE_13_17,
sum(c.AGE_35_44) AGE_35_44,
sum(c.AGE_55_64) AGE_55_64,
sum(c.AGE_65_) AGE_65_,
sum(c.AGE_18_24) AGE_18_24,
'UGC' as type from
__dbt__CTE__ugc_engage_measures a
left join 
__dbt__CTE__ugc_revenue_views_uploader_thirdparty b
on 
a.report_date=b.report_date
and a.country_code=b.country_code
left join __dbt__CTE__ugc_demograph_by_gender_age_group c
on a.report_date=c.report_date
and a.country_name=c.country_name
and a.country_code = c.country_code
group by a.report_date,a.report_date_dt, a.region2, a.country_name2, a.country_name,a.country_code