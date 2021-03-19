

with __dbt__CTE__intm_yt_daily_wwe_video_pastmonth as (


select a.time_uploaded,a.video_id as video_id, a.region_code as region_code,
a.ttl_views as ttl_views,a.views_30days,b.talent as talent
from
(select time_uploaded,video_id
,sum(views) as ttl_views, sum(case when datediff('day',time_uploaded,report_date_dt)<=30 then views end) as views_30days
,case when country_code in ('GF','UY','BQ','HT','PA','HN','CR','BS','MS','MQ','BL','BR','DM','VG','LC','BB','PY',
        'FK','VE','TT','VI','GD','SV','AG','BM','BZ','JM','CU','AR','CO','SR','EC','GT','KY','AI','GY','BO','CL','PE','AW',
        'NI','DO','GP','TC','VC')
		then 'LATAM' 
	when country_code in ('TW','HK','CN')	
		then 'CHINA'
		else country_code end as region_code
from "entdwdb"."fds_yt"."rpt_yt_wwe_engagement_daily"
where date(time_uploaded) > date(getdate()-30)  
and country_code in ('US','CA','IN','TW','HK','CN','GB','DE','UK','AU','MX','GF','UY','BQ','HT','PA','HN','CR','BS','MS',
        'MQ','BL','BR','DM','VG','LC','BB','PY','FK','VE','TT','VI','GD','SV','AG','BM','BZ','JM','CU','AR','CO','SR',
        'EC','GT','KY','AI','GY','BO','CL','PE','AW','NI','DO','GP','TC','VC')
group by time_uploaded,video_id, region_code
) a
left join
(select 
distinct yt_id, talent
from "entdwdb"."fds_yt"."yt_amg_content_groups"
) b
on a.video_id=b.yt_id
where b.talent is not null
and regexp_count(talent, ',') + 1 <=5
),  __dbt__CTE__intm_yt_daily_wwe_talent_pastmonth as (


with cte as 
(select n::int from
  (select row_number() over (order by true) as n from __dbt__CTE__intm_yt_daily_wwe_video_pastmonth)
cross join
(select  max(regexp_count(talent, '[,]')) as max_num from __dbt__CTE__intm_yt_daily_wwe_video_pastmonth) where n <= max_num + 1)
select distinct time_uploaded, trim(SPLIT_PART(talent,',',n)) as talent, video_id,
 region_code, ttl_views,views_30days
 from __dbt__CTE__intm_yt_daily_wwe_video_pastmonth
cross join cte
where  split_part(talent,',',n) is not null and split_part(talent,',',n) != ''
)select region_code, talent, 'past month' as granularity,
sum(ttl_views) as total_views, sum(views_30days) as views_30days, 
count(distinct video_id) as cnt_video_id
from __dbt__CTE__intm_yt_daily_wwe_talent_pastmonth
group by 1,2,3
order by 1 desc, 2,3 asc