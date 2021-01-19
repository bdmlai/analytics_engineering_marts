{{
  config({
		"materialized": 'ephemeral'
  })
}}


select  year||'-'||case when length(month) =1 then '0'||(month :: varchar(2)) else( month :: varchar(2)) end ||'-01' as month,region_nm,
  country_nm,gain as china_gain,followers as china_followers from 
(
select  
b.cal_year as year,b.cal_mth_num as month,
'CHINA' as country_nm, 'APAC' AS region_nm,
total_social_followers- case when lag(total_social_followers) over (order by c.Max_source_as_on_date) is null then 0 else lag(total_social_followers)
 over (order by c.Max_source_as_on_date) end as gain,
total_social_followers as followers from 
 {{source('hive_udl_chscl','china_weekly_social_data')}} a left join
{{source('cdm','dim_date')}} b on  trunc(a.source_as_on_date) = b.full_date
 join
 (
Select max(source_as_on_date) as Max_source_as_on_date,
max(Start_date) as max_start_Date,
b.cal_year as year,b.cal_mth_num as month
 from {{source('hive_udl_chscl','china_weekly_social_data')}} a left join
{{source('cdm','dim_date')}} b on  trunc(a.source_as_on_date) = b.full_date
 group by 3,4
 ) c
 on a.source_as_on_Date = c.Max_source_as_on_date and
 a.start_date=c.max_Start_date and
 month= c.month and
 year=c.year
 )
