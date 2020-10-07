
with __dbt__CTE__wwe_engagement_measures as (



select  x.report_date_dt,x.report_date,case when x.channel_name is null then 'Unknown' else x.channel_name end as channel_name,x.country_code,
case when x.country_code='US' then 'United States'
     when x.country_code='IN' then 'India'
     when x.country_code='GB' then 'United Kingdom'
else 'ROW' end as country_name,
case when x.duration<=60 then '1_Under 1 Minute'
     when x.duration<=120 then '2_1-2 Minutes'
	 when x.duration<=240 then '3_2-4 Minutes'
	 when x.duration<=360 then '4_4-6 Minutes'
	 when x.duration<=480 then '5_6-8 Minutes'
     when x.duration<=600 then '6_8-10 Minutes'
     when x.duration<=1200 then '7_10-20 Minutes'
     when x.duration<=3600 then '8_20-60 Minutes'
     when x.duration>3600 then '9_Over 60 Minutes'
     else 'other' end as duration_group,
case when  report_date_dt-trunc(x.time_uploaded)<=7 then 'new'
else 'old' end as debut_type, 
case when y.amg_content_group in ('Full Match','Kickoff','Network','Originals','PPV Clips','Raw','SmackDown','The Bella Twins','UpUpDownDown','WWE Performance Center') 
then y.amg_content_group else 'Other' end as owned_class,
d.region_nm as region2,initcap(d.country_nm) as country_name2,
sum(x.views) views,
sum(x.watch_time_minutes) watch_time_minutes,
sum(x.watch_time_minutes)/60 as hours_watched,
sum(x.likes) likes,
sum(x.dislikes) dislikes,
sum(x.subscribers_gained) subscribers_gained, 
sum(x.subscribers_lost) subscribers_lost
from 
(select report_date_dt,report_date,channel_name,country_code,duration,time_uploaded,video_id,views,watch_time_minutes,likes,dislikes,subscribers_gained,
subscribers_lost,dim_source_type_id,title from "entdwdb"."fds_yt"."rpt_yt_wwe_engagement_daily" where report_date_dt 
between current_date - 52 and current_date - 1 and dim_source_type_id in (2,3)) x
left join (select distinct yt_id,channel_name,amg_content_group from "entdwdb"."public"."yt_amg_content_groups") y 
/* 5/15/2020/ Hima / added distinct on amg content groups to eliminate duplicates */   
on x.video_id=y.yt_id and x.channel_name=y.channel_name  
/* 5/15/2020 / Hima / added join on channel_name along with video_id due to duplicates with NULL channel name in yt_amg_content_group table for some video_id's */
left join
(select distinct upper(iso_alpha2_ctry_cd) as iso_alpha2_ctry_cd ,country_nm,region_nm from "entdwdb"."cdm"."dim_region_country"
where etl_source_name='Youtube') d
on x.country_code=d.iso_alpha2_ctry_cd
where x.report_date_dt between current_date - 52 and current_date - 1
and dim_source_type_id in (2,3)
group by 1,2,3,4,5,6,7,8,9,10
),  __dbt__CTE__wwe_revenue_views_uploader_self as (


select r.report_date,case when m.channel_name is null then 'Unknown' else m.channel_name end as channel_name,r.country_code,
case when r.country_code='US' then 'United States'
     when r.country_code='IN' then 'India'
     when r.country_code='GB' then 'United Kingdom'
else 'ROW' end as country_name,
case when v.length<=60 then '1_Under 1 Minute'
     when v.length<=120 then '2_1-2 Minutes'
	 when v.length<=240 then '3_2-4 Minutes'
	 when v.length<=360 then '4_4-6 Minutes'
	 when v.length<=480 then '5_6-8 Minutes'
     when v.length<=600 then '6_8-10 Minutes'
     when v.length<=1200 then '7_10-20 Minutes'
     when v.length<=3600 then '8_20-60 Minutes'
     when v.length>3600 then '9_Over 60 Minutes'
     else 'other' end as duration_group,
case when  cast (substring(report_date,1,8) as date)-trunc(v.time_uploaded)<=7 then 'new'
else 'old' end as debut_type, 
case when y.amg_content_group in ('Full Match','Kickoff','Network','Originals','PPV Clips','Raw','SmackDown','The Bella Twins','UpUpDownDown','WWE Performance Center') 
then y.amg_content_group else 'Other' end as owned_class,
sum(r.ad_impressions) ad_impressions, sum(r.estimated_youtube_ad_revenue) estimated_youtube_ad_revenue, 
sum(r.estimated_partner_revenue) estimated_partner_revenue,
sum(case when r.video_id+r.report_date in 
(select distinct video_id+report_date from  "entdwdb"."fds_yt"."rpt_yt_revenue_daily" where estimated_partner_revenue!=0 group by video_id, report_date)
then views else 0 end) revenue_views
from (select report_date,country_code,ad_impressions,estimated_youtube_ad_revenue,estimated_partner_revenue,video_id,views,uploader_type
from "entdwdb"."fds_yt"."rpt_yt_revenue_daily" where report_date between to_char(current_date - 52, 'YYYYMMDD')   
and to_char(current_date - 1, 'YYYYMMDD') and uploader_type in ('self' ,'thirdParty')) r join fds_yt.dim_video v on r.video_id=v.video_id
left join 
(select distinct video_id, channel_name from  "entdwdb"."fds_yt"."youtube_video_metadata_direct" 
 where as_on_date=(select max(as_on_date) from "entdwdb"."fds_yt"."youtube_video_metadata_direct")) m
 on r.video_id=m.video_id
left join (select distinct yt_id,channel_name,amg_content_group from "entdwdb"."public"."yt_amg_content_groups")  y
/* 5/15/2020 / Hima /added join on channel_name along with video_id due to duplicates with NULL channel name in yt_amg_content_group table for some video_id's */
on r.video_id=y.yt_id and m.channel_name=y.channel_name
/* 5/15/2020 / Hima /added distinct on amg content groups to eliminate duplicates */
where r.report_date between to_char(current_date - 52, 'YYYYMMDD')  and to_char(current_date - 1, 'YYYYMMDD')
and r.uploader_type='self' 
group by 1, 2,3,4,5,6,7
),  __dbt__CTE__wwe_demograph_by_gender_age_group as (


select report_date, case when d.channel_name is null then 'Unknown' else d.channel_name end as channel_name, country_code,
case when d.country_code='US' then 'United States'
     when d.country_code='IN' then 'India'
     when d.country_code='GB' then 'United Kingdom'
else 'ROW' end as country_name,
case when d.duration<=60 then '1_Under 1 Minute'
     when d.duration<=120 then '2_1-2 Minutes'
	 when d.duration<=240 then '3_2-4 Minutes'
	 when d.duration<=360 then '4_4-6 Minutes'
	 when d.duration<=480 then '5_6-8 Minutes'
     when d.duration<=600 then '6_8-10 Minutes'
     when d.duration<=1200 then '7_10-20 Minutes'
     when d.duration<=3600 then '8_20-60 Minutes'
     when d.duration>3600 then '9_Over 60 Minutes'
     else 'other' end as duration_group,
case when  report_date_dt-trunc(d.time_uploaded)<=7 then 'new'
else 'old' end as debut_type, 
case when y.amg_content_group in ('Full Match','Kickoff','Network','Originals','PPV Clips','Raw','SmackDown','The Bella Twins','UpUpDownDown','WWE Performance Center') 
then y.amg_content_group else 'Other' end as owned_class,
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
from 
(select report_date,channel_name,country_code,duration,report_date_dt,time_uploaded,gender,age_group,views,video_id,uploader_type
from "entdwdb"."fds_yt"."rpt_yt_demographics_views_daily"   where report_date_dt between current_date-52 and current_date - 1 
and uploader_type in ( 'self' , 'thirdParty')) d 
left join (select distinct yt_id,channel_name,amg_content_group from "entdwdb"."public"."yt_amg_content_groups") y 
/* 5/15/2020 / Hima /added distinct on amg content groups to eliminate duplicates */
on d.video_id=y.yt_id and d.channel_name=y.channel_name
/* 5/15/2020 / Hima / added join on channel_name along with video_id due to duplicates with NULL channel name in yt_amg_content_group table for some video_id's */
where d.report_date_dt between current_date - 52 and current_date - 1 and d.uploader_type = 'self'
group by 1,2,3,4,5,6,7
)select a.report_date_dt,a.report_date,a.channel_name,a.country_name,a.country_code,a.duration_group,
a.debut_type,a.owned_class, a.region2, a.country_name2,
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
'Owned' as type from
 __dbt__CTE__wwe_engagement_measures  a
 left join
  __dbt__CTE__wwe_revenue_views_uploader_self b
  on a.report_date=b.report_date
and a.owned_class=b.owned_class
and a.debut_type=b.debut_type
and a.country_name=b.country_name
and a.country_code = b.country_code
and a.duration_group=b.duration_group
and a.channel_name=b.channel_name
left join
__dbt__CTE__wwe_demograph_by_gender_age_group c
on a.report_date=c.report_date
and a.owned_class=c.owned_class
and a.debut_type=c.debut_type
and a.country_name=c.country_name
and a.country_code = c.country_code
and a.duration_group=c.duration_group
and a.channel_name=c.channel_name
group by a.report_date_dt,a.report_date,a.channel_name,a.region2, a.country_name2,a.country_name,a.country_code,a.duration_group,a.debut_type,a.owned_class