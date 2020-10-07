


select report_date,report_date_dt,country_code,
case when country_code='US' then 'United States'
     when country_code='IN' then 'India'
     when country_code='GB' then 'United Kingdom'
     else 'ROW' end as country_name,
d.region_nm as region2,initcap(d.country_nm) as country_name2,
sum(views) views,sum(watch_time_minutes)/60 as hours_watched,
sum(watch_time_minutes) as watch_time_minutes,
sum(likes) likes,
sum(dislikes) dislikes,
sum(subscribers_gained) subscribers_gained, 
sum(subscribers_lost) subscribers_lost
from "entdwdb"."fds_yt"."rpt_yt_ugc_engagement_daily"  ue
left join
(select distinct upper(iso_alpha2_ctry_cd) as iso_alpha2_ctry_cd ,country_nm,region_nm from "entdwdb"."cdm"."dim_region_country"
where etl_source_name='Youtube') d
on ue.country_code=d.iso_alpha2_ctry_cd
where report_date_dt between current_date - 52 and current_date - 1 
and dim_source_type_id in (2,3)
group by 1,2,3,4,5,6