

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