

with display as 
(select  trunc(next_day(trunc(date)-1,'Su')) as week,
case when country= 'US' then 'USA'
when country = 'DE' then 'GERMANY'
when country = 'AU' or country = 'NZ' then 'AUS/NZ'
when country in ('IR' , 'EG' , 'SC', 'WS' , 'GB', 'JE','IM', 'IE','UK') then 'UK/IRE' else 'ROW' end as country,
'Paid Display' as vehicle,
case when split_part(placement,'_',8) in ('craveonline media','draftkings','reddit, inc','sublime skinz inc','wikia. inc') then 'Nurture'
when split_part(placement,'_',8) in ('tapjoy','xaxis','xaxisdirect','emea','exponential','mediaiq','miq','turbine','yieldmo','aol',
'bleacherreport.com','fite.tv','fyber','samsung electronics','viant us llc','vibrant media','ebuzzing','tapjoy','captify','programmaticmechanics',
'vdx','vdxl','samba','fandom') then 'Conversion' else split_part(placement,'_',8) end as level2, 
split_part(placement,'_',8) as level3, 
case when level3 = 'yieldmo' then 'Clicks' else 'Impressions' end as metric,
sum(impressions) as Impressions,
sum(actualized_spend) as Spend,
0 as Clicks
from "entdwdb"."hive_udl_mkt"."wavemaker_monthly_display_mmm"
group by 1,2,3,4,5,6),

search as 
(select trunc(next_day(trunc("from")-1,'Su')) as week,
case when geo_group = 'USA' then 'USA'
when geo_group = 'Germany' then 'GERMANY'
when geo_group in ('Australia','New Zealand') then 'AUS/NZ'
when geo_group in ('UK','Ireland','IR' , 'EG' , 'SC', 'WS' , 'GB', 'JE','IM', 'IE' ) then 'UK/IRE' else 'ROW' end as country,
'Paid Search' as vehicle,
case when publisher = 'Google AdWords' then 'Google' 
     when publisher = 'Bing Ads' then 'Bing' else publisher end as level2, 
'' as level3, 
'Clicks' as metric,
0 as Impressions,
sum(cost) as Spend,
sum(clicks) as Clicks
from "entdwdb"."hive_udl_mkt"."wavemaker_monthly_search_mmm"
group by 1,2,3,4,5,6),

youtube as
(select trunc(next_day(trunc(day)-1,'Su')) as week,
case when geography = 'US' then 'USA'
when geography = 'Germany' then 'GERMANY'
when geography in ('Australia','New Zealand') then 'AUS/NZ'
when geography in ('UK','Ireland','IR' , 'EG' , 'SC', 'WS' , 'GB', 'JE','IM', 'IE' ) then 'UK/IRE' else 'ROW' end as country,
'Paid Youtube' as vehicle,
'Youtube Trueview' as level2,
'' as level3, 
'Impressions' as metric,
sum(impressions) as Impressions,
sum(cost) as Spend,
0 as Clicks
from "entdwdb"."hive_udl_mkt"."wavemaker_monthly_youtube_mmm"
group by 1,2,3,4,5,6),

facebook as 
(select trunc(next_day(trunc(day)-1,'Su')) as week,
case when country = 'US' then 'USA'
when country = 'DE' then 'GERMANY'
when country in ('AU','NZ') then 'AUS/NZ'
when country in ('IR' , 'EG' , 'SC', 'WS' , 'GB', 'JE','IM', 'IE','UK' ) then 'UK/IRE' else 'ROW' end as country,
'Paid Social' as vehicle,
'Paid Facebook' as level2,
'Facebook' as level3, 
'Impressions' as metric,
sum(impressions) as Impressions,
sum(amount_spent_usd) as Spend,
0 as Clicks
from "entdwdb"."hive_udl_mkt"."wavemaker_monthly_facebook_mmm"
group by 1,2,3,4,5,6),

twitter as 
(select trunc(next_day(trunc(time_period)-1,'Su')) as week,
case when location = 'United States' then 'USA'
when location = 'DE' then 'GERMANY'
when location in ('AU','NZ') then 'AUS/NZ'
when location in ('IR' , 'EG' , 'SC', 'WS' , 'GB', 'JE','IM', 'IE','UK') then 'UK/IRE' else 'ROW' end as country,
'Paid Social' as vehicle,
'Paid Twitter' as level2,
'Twitter' as level3, 
'Impressions' as metric,
sum(impressions) as Impressions,
sum(spend) as Spend,
0 as Clicks
from "entdwdb"."hive_udl_mkt"."wavemaker_monthly_twitter_mmm"
group by 1,2,3,4,5,6),

snapchat as 
(select trunc(next_day(trunc(day)-1,'Su')) as week,
'USA' as country,
'Paid Social' as vehicle,
'Paid Snapchat' as level2,
'Snapchat' as level3, 
'Impressions' as metric,
sum(paid_impressions) as Impressions,
sum(spend) as Spend,
0 as Clicks
from "entdwdb"."hive_udl_mkt"."wavemaker_monthly_snapchat_mmm"
group by 1,2,3,4,5,6),

consolidation as 
(select * from display
union all select * from search
union all select * from youtube
union all select * from facebook
union all select * from twitter
union all select * from snapchat
order by week,country)


(select a.*,'Paid Media' as data_category,'All' as audience,
case when b.ppv_name is null then 'Non Go-Home Week' else b.ppv_name end as ppv_name,
case when b.ppv_type is null then 'Non Go-Home Week' else b.ppv_type end as ppv_type,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_NPLUS' etl_batch_id, 'bi_dbt_user_prd' AS etl_insert_user_id,
    SYSDATE                                   AS etl_insert_rec_dttm,
    CAST(NULL as VARCHAR)                     AS etl_update_user_id,
    CAST( NULL AS TIMESTAMP)                  AS etl_update_rec_dttm
from consolidation a
left join 
(select distinct 
trunc(next_day(trunc(premiere_date)-1,'Su')) as week,
case when episode_nm in ('WrestleMania 36 Part 1','WrestleMania 36 Part 2') then  'WrestleMania 36' else episode_nm end as ppv_name,
ppv_brand_name as ppv_type
from "entdwdb"."cdm"."dim_content_classification_title" where series_group='WWE PPV') b
on a.week = b.week
order by a.week)