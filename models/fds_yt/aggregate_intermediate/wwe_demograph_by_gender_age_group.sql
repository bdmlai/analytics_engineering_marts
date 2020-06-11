{{
  config(
	materialized='ephemeral'
  )
}}

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
from {{source('fds_yt','rpt_yt_demographics_views_daily')}}   where report_date_dt between current_date-52 and current_date - 1 
and uploader_type in ( 'self' , 'thirdParty')) d 
left join (select distinct yt_id,channel_name,amg_content_group from {{source('public','yt_amg_content_groups')}}) y 
/* 5/15/2020 / Hima /added distinct on amg content groups to eliminate duplicates */
on d.video_id=y.yt_id and d.channel_name=y.channel_name
/* 5/15/2020 / Hima / added join on channel_name along with video_id due to duplicates with NULL channel name in yt_amg_content_group table for some video_id's */
where d.report_date_dt between current_date - 52 and current_date - 1 and d.uploader_type = 'self'
group by 1,2,3,4,5,6,7

