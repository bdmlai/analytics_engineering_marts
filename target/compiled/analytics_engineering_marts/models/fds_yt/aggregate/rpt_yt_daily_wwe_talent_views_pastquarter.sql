

with __dbt__CTE__intm_yt_daily_wwe_video_pastquarter as (


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
),  __dbt__CTE__intm_yt_daily_wwe_talent_pastquarter as (


with cte as 
(select n::int from
  (select row_number() over (order by true) as n from __dbt__CTE__intm_yt_daily_wwe_video_pastquarter)
cross join
(select  max(regexp_count(talent, '[,]')) as max_num from __dbt__CTE__intm_yt_daily_wwe_video_pastquarter) where n <= max_num + 1)
select distinct time_uploaded, trim(SPLIT_PART(talent,',',n)) as talent, video_id,
 region_code, ttl_views,views_30days
 from __dbt__CTE__intm_yt_daily_wwe_video_pastquarter
cross join cte
where  split_part(talent,',',n) is not null and split_part(talent,',',n) != ''
)select region_code,initcap(country_nm) as region_name, talent, 'past quarter' as granularity,
sum(ttl_views) as total_views, sum(views_30days) as views_30days, 
count(distinct video_id) as cnt_video_id,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_content' as etl_batch_id,
 'bi_dbt_user_prd' as etl_insert_user_id, 
current_timestamp as etl_insert_rec_dttm, 
null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm

from __dbt__CTE__intm_yt_daily_wwe_talent_pastquarter a
inner join "entdwdb"."cdm"."dim_region_country"  b on a.region_code=upper(b.iso_alpha2_ctry_cd)
where b.ent_map_nm ='GM Region'
group by 1,2,3
order by 1 desc, 2,3 asc