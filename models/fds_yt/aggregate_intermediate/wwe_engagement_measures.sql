{{
  config(
	schema='fds_yt',
	materialized='ephemeral'
  )
}}


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
subscribers_lost,dim_source_type_id,title from {{source('fds_yt','rpt_yt_wwe_engagement_daily')}} where report_date_dt 
between current_date - 52 and current_date - 1 and dim_source_type_id in (2,3)) x
left join (select distinct yt_id,channel_name,amg_content_group from {{source('public','yt_amg_content_groups')}}) y 
/* 5/15/2020/ Hima / added distinct on amg content groups to eliminate duplicates */   
on x.video_id=y.yt_id and x.channel_name=y.channel_name  
/* 5/15/2020 / Hima / added join on channel_name along with video_id due to duplicates with NULL channel name in yt_amg_content_group table for some video_id's */
left join
(select distinct upper(iso_alpha2_ctry_cd) as iso_alpha2_ctry_cd ,country_nm,region_nm from {{source('cdm','dim_region_country')}}
where etl_source_name='Youtube') d
on x.country_code=d.iso_alpha2_ctry_cd
where x.report_date_dt between current_date - 52 and current_date - 1
and dim_source_type_id in (2,3)
group by 1,2,3,4,5,6,7,8,9,10
