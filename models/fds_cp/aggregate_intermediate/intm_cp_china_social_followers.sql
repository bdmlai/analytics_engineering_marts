{{
  config({
		"materialized": 'ephemeral'
  })
}}

select  b.cal_year as year,b.cal_mth_num as month,
'China_social' as Platform,
'CHINA' as country_nm, 'APAC' AS region_nm,
(weibo_total_followers +
wechat_total_wechat_followers  +
 wechat_total_qzone_followers  +
toutiao_total_followers +
 wechat_total_vplus_subscribers   +
 youku_total_followers) as followers from 
 {{source('hive_udl_chscl','china_weekly_social_data')}} a left join
{{source('cdm','dim_date')}} b on trunc(a.source_as_on_date) = b.full_date
 join
 (
Select max(source_as_on_date) as Max_source_as_on_date,
max(Start_date) as max_start_Date,
b.cal_year as year,b.cal_mth_num as month
 from {{source('hive_udl_chscl','china_weekly_social_data')}} a left join
{{source('cdm','dim_date')}} b on trunc(a.source_as_on_date) = b.full_date
 group by 3,4
 ) c
 on a.source_as_on_Date = c.Max_source_as_on_date and
 a.start_date=c.max_Start_date and
 month= c.month and
 year=c.year
 

 
