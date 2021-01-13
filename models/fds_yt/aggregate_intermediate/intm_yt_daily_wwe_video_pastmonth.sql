{{
  config({
		"materialized": 'ephemeral'
  })
}}

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
from {{source('fds_yt','rpt_yt_wwe_engagement_daily')}}
where date(time_uploaded) > date(getdate()-30)  
and country_code in ('US','CA','IN','TW','HK','CN','GB','DE','UK','AU','MX','GF','UY','BQ','HT','PA','HN','CR','BS','MS',
        'MQ','BL','BR','DM','VG','LC','BB','PY','FK','VE','TT','VI','GD','SV','AG','BM','BZ','JM','CU','AR','CO','SR',
        'EC','GT','KY','AI','GY','BO','CL','PE','AW','NI','DO','GP','TC','VC')
group by time_uploaded,video_id, region_code
) a
left join
(select 
distinct yt_id, talent
from {{source('fds_yt','yt_amg_content_groups')}}
) b
on a.video_id=b.yt_id
where b.talent is not null
and regexp_count(talent, ',') + 1 <=5  
