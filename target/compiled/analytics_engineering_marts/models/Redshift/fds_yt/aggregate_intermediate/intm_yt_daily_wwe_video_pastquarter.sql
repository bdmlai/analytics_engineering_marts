

select a.time_uploaded,a.video_id as video_id, a.region_code as region_code,
a.ttl_views as ttl_views,a.views_30days,b.talent as talent
from
(select time_uploaded,video_id
,sum(views) as ttl_views, sum(case when datediff('day',time_uploaded,report_date_dt)<=30 then views end) as views_30days
,country_code as region_code
from "entdwdb"."fds_yt"."rpt_yt_wwe_engagement_daily"
where date(time_uploaded) > date(getdate()-90)  
and country_code in ('AU','AT','BD','BR','CA','CL','EG','FR','DE','HK','IN','ID','IQ','IT','MY','MX','NP',
                         'NZ','PK','PH','PT','RU','SA',	'SG','ZA','ES',	'SE','CH','TW',	'TH','TR','AE',	'GB','US','VN')
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