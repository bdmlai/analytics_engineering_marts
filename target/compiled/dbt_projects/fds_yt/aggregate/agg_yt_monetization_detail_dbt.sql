
select * from (select a.*,
row_number() over (partition by a.date,a.country_code,a.region2 order by a.views desc) as viewrank,
row_number() over (partition by a.date,a.country_code,a.region2 order by a.watch_time_mins desc) as watchrank,
row_number() over (partition by a.date,a.country_code,a.region2 order by a.likes desc) as likerank,
row_number() over (partition by a.date,a.country_code,a.region2 order by a.dislikes desc) as dislikerank,
row_number() over (partition by a.date,a.country_code,a.region2 order by a.ad_impressions desc) as adrank,
row_number() over (partition by a.date,a.country_code,a.region2 order by a.estimated_partner_revenue desc) as revenuerank,
sysdate insert_timestamp from 
(select * from dwh_read_write.Agg_YT_monetization_base 
union all
select * from (select a.*, b.ad_impressions, b.estimated_partner_revenue, b.estimated_partner_ad_revenue, c.debut_date
 from 
(select report_date_dt date, 
report_date, video_id,Title,video_uploaded_flg, 
channel_name,region_nm as region2, 
case when country_code in ('IN','US','ID','GB','SA','PK','PH',
'MX','VN','TH','MY','DE','TR','BR','BD','EG','AE','IT','CA','FR') then country_code else 'ZZ' end as country_code, 
case when country_code in ('IN','US','ID','GB','SA','PK','PH',
'MX','VN','TH','MY','DE','TR','BR','BD','EG','AE','IT','CA','FR') then country_name else 'Other' end as country_name,
'Owned' as content_type, 
sum(views) views, sum(likes) likes, sum(dislikes) dislikes, sum(watch_time_minutes) watch_time_mins
from 
(select a.*,d.* 
from 
(select * from fds_yt.rpt_yt_wwe_engagement_daily where report_date between 20190101 and cast(to_char(current_date,'YYYYMMDD') as int)) a 
left join
(select distinct upper(iso_alpha2_ctry_cd) as iso_alpha2_ctry_cd ,initcap(country_nm) as country_name,region_nm from cdm.dim_region_country
where etl_source_name='Youtube')d 
on a.country_code=d.iso_alpha2_ctry_cd) group by 1,2,3,4,5,6,7,8,9,10) a
left join 
(select video_id, report_date, case when country_code in ('IN','US','ID','GB','SA','PK','PH',
'MX','VN','TH','MY','DE','TR','BR','BD','EG','AE','IT','CA','FR') then country_code else 'ZZ' end as country_code,
region_nm as region2,
nvl(sum(ad_impressions),0) ad_impressions, nvl(sum(estimated_partner_revenue),0) estimated_partner_revenue, nvl(sum(estimated_partner_ad_revenue),0) estimated_partner_ad_revenue
from 
(select a.*,d.* from (select * from fds_yt.rpt_yt_revenue_daily where report_date between 20190101 and cast(to_char(current_date,'YYYYMMDD') as int)) a 
left join
(select distinct upper(iso_alpha2_ctry_cd) as iso_alpha2_ctry_cd ,initcap(country_nm) as country_name,region_nm from cdm.dim_region_country
where etl_source_name='Youtube')d on a.country_code=d.iso_alpha2_ctry_cd) group by 1,2,3,4) b
on a.video_id=b.video_id and a.report_date=b.report_date and a.country_code=b.country_code and a.region2=b.region2
left join (select distinct report_date_dt debut_date, video_id from fds_yt.rpt_yt_wwe_engagement_daily where video_uploaded_flg=1 ) c
on a.video_id=c.video_id)) a) where (viewrank<20 or watchrank<20 or likerank<20 or dislikerank<20 or adrank<20 or revenuerank<20)