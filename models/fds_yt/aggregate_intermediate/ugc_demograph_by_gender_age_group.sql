{{
  config(
	schema='fds_yt',
	materialized='ephemeral'
  )
}}


select report_date, country_code,
case when d.country_code='US' then 'United States'
     when d.country_code='IN' then 'India'
     when d.country_code='GB' then 'United Kingdom'
else 'ROW' end as country_name,
coalesce(sum(case when gender='MALE' then views*1000000 else 0 end)/nullif(sum(views),0),0) as male,
coalesce(sum(case when gender='FEMALE' then views*1000000 else 0 end)/nullif(sum(views),0),0) as female,
coalesce(sum(case when gender='GENDER_OTHER' then views*1000000 else 0 end)/nullif(sum(views),0),0) as gender_other,
coalesce(sum(case when age_group='AGE_25_34' then views*1000000 else 0 end)/nullif(sum(views),0),0) as AGE_25_34,
coalesce(sum(case when age_group='AGE_45_54' then views*1000000 else 0 end)/nullif(sum(views),0),0) as AGE_45_54,
coalesce(sum(case when age_group='AGE_13_17' then views*1000000 else 0 end)/nullif(sum(views),0),0) as AGE_13_17,
coalesce(sum(case when age_group='AGE_35_44' then views*1000000 else 0 end)/nullif(sum(views),0),0) as AGE_35_44,
coalesce(sum(case when age_group='AGE_55_64' then views*1000000 else 0 end)/nullif(sum(views),0),0) as AGE_55_64,
coalesce(sum(case when age_group='AGE_65_' then views*1000000 else 0 end)/nullif(sum(views),0),0) as AGE_65_,
coalesce(sum(case when age_group='AGE_18_24' then views*1000000 else 0 end)/nullif(sum(views),0),0) as AGE_18_24
from (select report_date,channel_name,country_code,duration,report_date_dt,time_uploaded,gender,age_group,views,video_id,uploader_type
from {{source('fds_yt','rpt_yt_demographics_views_daily')}} where report_date_dt between current_date-52 and current_date - 1 and uploader_type in ( 'self' , 'thirdParty')) d 
where d.report_date_dt between current_date - 52 and current_date - 1  and d.uploader_type = 'thirdParty'
group by 1,2,3
