 {{
  config({
	"schema": 'fds_mkt',	
	"materialized": 'incremental',
	"pre-hook":"delete from fds_mkt.rpt_mkt_weekly_owned_media_execution"
		})
}}
with yt_card_impressions as
(select  trunc(next_day(trunc(report_date)-1,'Su')) as week,
case when country_code= 'US' then 'USA'
when country_code = 'DE' then 'GERMANY'
when country_code = 'AU' or country_code = 'NZ' then 'AUS/NZ'
when country_code in ('IR' , 'EG' , 'SC', 'WS' , 'GB', 'JE','IM', 'IE' ) then 'UK/IRE' else 'ROW' end as country,
'Owned Youtube' as vehicle,'Cards' as level2, '' as level3  , 'Impressions' as metric ,
sum(card_teaser_impressions) as exposure
from {{source('fds_yt','youtube_cards')}}
where card_type= 68 AND uploader_type = 'self' 
group by 1,2,3,4,5,6),

yt_annotation_impressions as
(select  trunc(next_day(trunc(report_date)-1,'Su')) as week,
case when country_code= 'US' then 'USA'
when country_code = 'DE' then 'GERMANY'
when country_code = 'AU' or country_code = 'NZ' then 'AUS/NZ'
when country_code in ('IR' , 'EG' , 'SC', 'WS' , 'GB', 'JE','IM', 'IE' ) then 'UK/IRE' else 'ROW' end as country,
'Owned Youtube' as vehicle,'Annotations' as level2, '' as level3  , 'Impressions' as metric,
sum(annotation_impressions) as exposure
from {{source('fds_yt','youtube_annotations')}}
where annotation_type='30' AND uploader_type = 'self'
group by 1,2,3,4,5,6),

yt_end_screen_impressions as
(select  trunc(next_day(trunc(report_date)-1,'Su')) as week,
case when country_code= 'US' then 'USA'
when country_code = 'DE' then 'GERMANY'
when country_code = 'AU' or country_code = 'NZ' then 'AUS/NZ'
when country_code in ('IR' , 'EG' , 'SC', 'WS' , 'GB', 'JE','IM', 'IE' ) then 'UK/IRE' else 'ROW' end as country,
'Owned Youtube' as vehicle,'EndScreen' as level2, '' as level3 , 'Impressions' as metric,
sum(end_screen_element_impressions) as exposure
from {{source('fds_yt','youtube_end_screens')}}
where end_screen_element_type = '506' AND uploader_type = 'self'
group by 1,2,3,4,5,6),

fb_impressions as 
(select trunc(next_day(trunc(post_date)-1,'Su')) as week,
'Global' as country,'Owned Social' as vehicle,'Network' as level2, 'Facebook' as level3 , 'Impressions' as metric,
sum(impressions_total) as exposure from
(select * from {{source('fds_fbk','fact_fb_consumption_post')}} where lower(post_text)  like '%network%')
group by 1,2,3,4,5,6),

tw_impressions as
(select trunc(next_day(trunc(post_date)-1,'Su')) as week,
'Global' as country,'Owned Social' as vehicle,'Network' as level2, 'Twitter' as level3 , 'Impressions' as metric,
sum(impressions) as exposure from
(select distinct * from {{source('fds_tw','fact_tw_consumption_post')}} where lower(tweet)  like '%network%')
group by 1,2,3,4,5,6),

ig_impressions as
(select trunc(next_day(trunc(post_date)-1,'Su')) as week,
'Global' as country,'Owned Social' as vehicle,'Network' as level2, 'Instagram' as level3 , 'Impressions' as metric,
sum(impressions) as exposure from
(select distinct * from {{source('fds_igm','fact_ig_consumption_post')}} where lower(caption)  like '%network%')
group by 1,2,3,4,5,6),

owned_tv_us_vshp as
(select trunc(next_day(trunc(cast(cast(orig_broadcast_date_id as varchar) as date))-1,'Su')) as week,
'USA' as country,'Owned TV' as vehicle,'Owned TV Viewership' as level2,
case when src_program_name='WWE ENTERTAINMENT' then 'RAW' 
when src_program_name like '%WWE FRI NIGHT SMACKDOWN%' then 'SMACKDOWN'
when src_program_name='WWE NXT' then 'NXT' end as level3,
'Viewership' as metric,
avg(most_current_us_audience_avg_proj_000) as exposure
from {{source('fds_nl','fact_nl_minxmin_ratings')}} where  
src_demographic_group='Persons 2 - 99' and src_playback_period_cd='Live+SD | TV with Digital | Linear with VOD'
and src_program_name in ('WWE ENTERTAINMENT','WWE FRI NIGHT SMACKDOWN','WWE FRI NIGHT SMACKDOWN L','WWE NXT')
group by 1,2,3,4,5,6),

owned_tv_promos as
(select distinct trunc(next_day(trunc(airdate)-1,'Su')) as week,
'Global' as country,'Owned TV' as vehicle,
case when lower(segmenttype) like  'announcer on camera' and network_flag = 1 then 'Paid Tier TV Promos Count Announcer on Camera'
when (lower(segmenttype) ='promo' or lower(segmenttype) like '%sponsor element%' ) and network_flag = 1 then 'Paid Tier TV Promos Count Promo'
when lower(segmenttype) like '%lower third%' and network_flag = 1 then 'Paid Tier TV Promos Count Lower Third' 
when lower(segmenttype) ='promo graphic' and network_flag = 1 then 'Paid Tier TV Promos Count Promo Graphic'

when lower(segmenttype) like  'announcer on camera' and freetier_flag = 1 then 'Free Tier TV Promos Count Announcer on Camera'
when (lower(segmenttype) ='promo' or lower(segmenttype) like '%sponsor element%' ) and freetier_flag = 1  then 'Free Tier TV Promos Count Promo'
when lower(segmenttype) like '%lower third%' and freetier_flag = 1  then 'Free Tier TV Promos Count Lower Third' 
when lower(segmenttype) ='promo graphic' and freetier_flag = 1  then 'Free Tier TV Promos Count Promo Graphic'
else lower(segmenttype) end as level2,
upper(show_type) as level3,'Number of Promos' as metric,
count ((lower(sponsors))) as exposure
from 
(select cast(title as varchar(512)) as show_type, cast(showdbid as varchar(512)) as fileid , airdate, segmenttype, comment ,sponsors,
case when (lower(sponsors) like '%network%' and lower(sponsors) not like '%free tier%') then 1 else 0 end as network_flag,
case when lower(sponsors) like '%free tier%' then 1 else 0 end as freetier_flag
from {{source('udl_nplus','raw_lite_log')}}
where (lower(segmenttype) like  'announcer on camera' or lower(segmenttype) like '%promo%' or lower(segmenttype) like '%sponsor element%' 
or lower(segmenttype) like '%lower third%')
and (lower(sponsors) like '%network%' or lower(sponsors) like '%free tier%') 
and lower(title) in ('smackdown' ,'raw','nxt'))
group by 1,2,3,4,5,6),

owned_tv_impressions as
(select a.week,'USA' as country,'Owned TV' as vehicle,
case when b.level2 = 'Paid Tier TV Promos Count Announcer on Camera' then 'Owned TV Paid Tier Announcer on Camera'
when b.level2 = 'Paid Tier TV Promos Count Lower Third' then 'Owned TV Paid Tier Lower Third'
when b.level2 = 'Paid Tier TV Promos Count Promo' then 'Owned TV Paid Tier Promo'
when b.level2 = 'Paid Tier TV Promos Count Promo Graphic' then 'Owned TV Paid Tier Promo Graphic' 
when b.level2 = 'Free Tier TV Promos Count Announcer on Camera' then 'Owned TV Free Tier Announcer on Camera'
when b.level2 = 'Free Tier TV Promos Count Lower Third' then 'Owned TV Free Tier Lower Third'
when b.level2 = 'Free Tier TV Promos Count Promo' then 'Owned TV Free Tier Promo'
when b.level2 = 'Free Tier TV Promos Count Promo Graphic' then 'Owned TV Free Tier Promo Graphic' 
end as level2,
a.level3,
'Impressions' as metric,a.exposure*b.exposure as exposure
from owned_tv_us_vshp a
left join owned_tv_promos b
on a.week = b.week
and a.level3 = b.level3),

owned_tv_nonus_vshp as
(select trunc(next_day(trunc(cast(broadcast_date as date))-1,'Su')) as week,
case when src_country in ( 'united kingdom', 'ireland') then 'UK/IRE'
when src_country in  ('australia', 'new zealand') then 'AUS/NZ' 
when src_country in ('germany') then 'GERMANY'else 'ROW' end as country,
'Owned TV' as vehicle,'Owned TV Viewership' as level2,
upper(series_name) as level3,'Viewership' as metric,
sum(aud) as exposure
from {{source('fds_kntr','fact_kntr_wwe_telecast_data')}}
where demographic_type='Everyone' and live_flag='Y'
group by 1,2,3,4,5,6),

owned_tv_nonus_impressions as
(select a.week,a.country,'Owned TV' as vehicle,
case when b.level2 = 'Paid Tier TV Promos Count Announcer on Camera' then 'Owned TV Paid Tier Announcer on Camera'
when b.level2 = 'Paid Tier TV Promos Count Lower Third' then 'Owned TV Paid Tier Lower Third'
when b.level2 = 'Paid Tier TV Promos Count Promo' then 'Owned TV Paid Tier Promo'
when b.level2 = 'Paid Tier TV Promos Count Promo Graphic' then 'Owned TV Paid Tier Promo Graphic' 

when b.level2 = 'Free Tier TV Promos Count Announcer on Camera' then 'Owned TV Free Tier Announcer on Camera'
when b.level2 = 'Free Tier TV Promos Count Lower Third' then 'Owned TV Free Tier Lower Third'
when b.level2 = 'Free Tier TV Promos Count Promo' then 'Owned TV Free Tier Promo'
when b.level2 = 'Free Tier TV Promos Count Promo Graphic' then 'Owned TV Free Tier Promo Graphic' 
end as level2,
a.level3,
'Impressions' as metric,a.exposure*b.exposure as exposure
from owned_tv_nonus_vshp a
left join owned_tv_promos b
on a.week = b.week
and a.level3 = b.level3),

owned_tv_ppv_vshp as
(select trunc(next_day(trunc(event_date)-1,'Su')) as week,
'Global' as country,
'Owned TV' as vehicle,
'Owned TV Viewership' as level2,
'PPV Kick-Off' as level3,
'Viewership' as metric,
sum(views) as exposure
from {{source('fds_nplus','rpt_network_ppv_liveplusvod')}}
where event_brand='PPV' and platform not in ('Total','Network') and data_level='Live+VOD'
group by 1,2,3,4,5,6),

owned_tv_ppv_promos as
(select distinct trunc(next_day(trunc(airdate)-1,'Su')) as week,
'Global' as country,'Owned TV' as vehicle,
'TV Promos Count PPV Kick-Off' as level2,
'PPV Kick-Off' as level3,'Number of Promos' as metric,
sum(network_flag) as exposure
from 
(select cast(title as varchar(512)) as show_type, cast(showdbid as varchar(512)) as fileid , airdate, segmenttype, comment ,sponsors,
case when lower(sponsors) like '%network%' then 1 else 0 end as network_flag
from {{source('udl_nplus','raw_lite_log')}}
where (lower(segmenttype) like  'announcer on camera' or lower(segmenttype) like '%promo%' or lower(segmenttype) like '%sponsor element%' 
or lower(segmenttype) like '%lower third%')
and lower(sponsors) like '%network%' and lower(sponsors) not like '%free tier%' and lower(title) in ('smackdown' ,'raw','nxt'))
group by 1,2,3,4,5,6),

owned_tv_ppv_impressions as
(select a.week,'Global' as country,'Owned TV' as vehicle,
'TV Promos Count PPV Kick-Off' as level2,
a.level3,
'Impressions' as metric,a.exposure*b.exposure as exposure
from owned_tv_ppv_vshp a
left join owned_tv_ppv_promos b
on a.week = b.week
and a.level3 = b.level3),

final as (
select * from yt_card_impressions
union all select * from yt_annotation_impressions
union all select * from yt_end_screen_impressions
union all select * from fb_impressions
union all select * from tw_impressions
union all select * from ig_impressions
union all select * from owned_tv_us_vshp
union all select * from owned_tv_promos
union all select * from owned_tv_impressions
union all select * from owned_tv_nonus_vshp
union all select * from owned_tv_nonus_impressions
union all select * from owned_tv_ppv_vshp
union all select * from owned_tv_ppv_promos
union all select * from owned_tv_ppv_impressions
order by week)

select a.*,'Owned Media' as data_category,'All' as audience,
case when b.ppv_name is null then 'Non Go-Home Week' else b.ppv_name end as ppv_name,
case when b.ppv_type is null then 'Non Go-Home Week' else b.ppv_type end as ppv_type,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_NPLUS' etl_batch_id, 'bi_dbt_user_prd' AS etl_insert_user_id,
    SYSDATE                                   AS etl_insert_rec_dttm,
    CAST(NULL as VARCHAR)                     AS etl_update_user_id,
    CAST( NULL AS TIMESTAMP)                  AS etl_update_rec_dttm
from final a
left join 
(select distinct 
trunc(next_day(trunc(premiere_date)-1,'Su')) as week,
case when episode_nm in ('WrestleMania 36 Part 1','WrestleMania 36 Part 2') then  'WrestleMania 36' else episode_nm end as ppv_name,
ppv_brand_name as ppv_type
from {{source('cdm','dim_content_classification_title')}} where series_group='WWE PPV') b
on a.week = b.week
order by a.week