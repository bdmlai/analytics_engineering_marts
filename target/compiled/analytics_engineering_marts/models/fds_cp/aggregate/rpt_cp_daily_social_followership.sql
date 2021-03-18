


with __dbt__CTE__intm_cp_fb_followers_gain as (


select year||'-'||case when length(month) =1 then '0'||(month :: varchar(2)) else( month :: varchar(2)) end ||'-01' as month,region_nm,
regexp_replace(initcap(country_nm),'[^0-9A-z ,\\.()\\-]','') as country_nm,sum(gain) as fb_gain,sum(followers) as fb_followers from
(
select b.cal_year as year,b.cal_mth_num as month,
nvl(f.region_nm,'Other') as region_nm,nvl(f.country_nm,'Other') as country_nm,
d.account_name,(c_followers) as followers ,(h.dod_followers) as gain
from "entdwdb"."fds_fbk"."fact_fb_smfollowership_audience_bycountry"  a  left join
"entdwdb"."cdm"."dim_smprovider_account" d on a.dim_Smprovider_account_id = d.dim_smprovider_account_id
left join ( select distinct country_nm,dim_country_id,region_nm from "entdwdb"."cdm"."dim_region_country" where ent_map_nm = 'GM Region' )f
on a.dim_country_id = f.dim_Country_id left join
"entdwdb"."cdm"."dim_date" b on a.dim_date_id = b.dim_date_id
 join
(
select b.cal_year as year,b.cal_mth_num as month,
dim_country_id,d.account_name,sum(dod_followers) as dod_followers ,max(a.dim_date_id) as last_day from
"entdwdb"."fds_fbk"."fact_fb_smfollowership_audience_bycountry"  a  left join
"entdwdb"."cdm"."dim_smprovider_account" d on a.dim_Smprovider_account_id = d.dim_smprovider_account_id left join
"entdwdb"."cdm"."dim_date" b on a.dim_date_id = b.dim_date_id
group by 1,2,3,4
) h 
on MONTH=h.month and 
YEAR=h.year and
a.dim_country_id=h.dim_country_id and
d.account_name=h.account_name
and a.dim_date_id=h.last_day
)
group by 1,2,3
),  __dbt__CTE__intm_cp_ig_followers_gain as (



select year||'-'||case when length(month) =1 then '0'||(month :: varchar(2)) else( month :: varchar(2)) end ||'-01' as month,region_nm,
regexp_replace(initcap(country_nm),'[^0-9A-z ,\\.()\\-]','') as country_nm,sum(gain) as ig_gain,sum(followers) as ig_followers from (
select b.cal_year as year,b.cal_mth_num as month,
nvl(f.region_nm,'Other') as region_nm,nvl(f.country_nm,'Other') as country_nm,
d.account_name,h.dod_followers as gain,
 (c_followers) as followers
 from "entdwdb"."fds_igm"."fact_ig_smfollowership_audience_bycountry" a left join
"entdwdb"."cdm"."dim_smprovider_account" d on a.dim_Smprovider_account_id = d.dim_smprovider_account_id
left join ( select distinct country_nm,dim_country_id,region_nm from "entdwdb"."cdm"."dim_region_country" where ent_map_nm = 'GM Region')f
 on a.dim_country_id = f.dim_Country_id left join
"entdwdb"."cdm"."dim_date" b on a.dim_date_id = b.dim_date_id
 join
(
select b.cal_year as year,b.cal_mth_num as month,
dim_country_id,d.account_name,sum(dod_followers)  as dod_followers, max(a.dim_date_id) as last_day from
"entdwdb"."fds_igm"."fact_ig_smfollowership_audience_bycountry" a left join
"entdwdb"."cdm"."dim_smprovider_account" d on a.dim_Smprovider_account_id = d.dim_smprovider_account_id left join
"entdwdb"."cdm"."dim_date" b on a.dim_date_id = b.dim_date_id
group by 1,2,3,4
) h 
on month=h.month and 
year=h.year and
a.dim_country_id=h.dim_country_id  and
d.account_name=h.account_name
and a.dim_date_id=h.last_day
)
group by 1,2,3
),  __dbt__CTE__intm_cp_yt_followers_gain as (



select 
trunc(date_trunc('month',trunc(convert(datetime,convert(varchar(10),report_date))))) as month,
nvl(region2,'Other') as region_nm,
 nvl(regexp_replace(initcap(country_name2),'[^0-9A-z ,\\.()\\-]',''),'Other') as country_nm,
sum(subscribers_gained) - sum(subscribers_lost) as yt_gain,
 sum(yt_gain) over (partition by region_nm,country_nm order by month asc rows between unbounded preceding and current row) as yt_followers
from "entdwdb"."fds_yt"."agg_yt_monetization_summary"
group by 1,2,3
order by 1,2,3 desc
),  __dbt__CTE__intm_cp_china_followers_gain as (



select  year||'-'||case when length(month) =1 then '0'||(month :: varchar(2)) else( month :: varchar(2)) end ||'-01' as month,region_nm,
  country_nm,gain as china_gain,followers as china_followers from 
(
select  
b.cal_year as year,b.cal_mth_num as month,
'CHINA' as country_nm, 'APAC' AS region_nm,
total_social_followers- case when lag(total_social_followers) over (order by c.Max_source_as_on_date) is null then 0 else lag(total_social_followers)
 over (order by c.Max_source_as_on_date) end as gain,
total_social_followers as followers from 
 "entdwdb"."hive_udl_chscl"."china_weekly_social_data" a left join
"entdwdb"."cdm"."dim_date" b on  trunc(a.source_as_on_date) = b.full_date
 join
 (
Select max(source_as_on_date) as Max_source_as_on_date,
max(Start_date) as max_start_Date,
b.cal_year as year,b.cal_mth_num as month
 from "entdwdb"."hive_udl_chscl"."china_weekly_social_data" a left join
"entdwdb"."cdm"."dim_date" b on  trunc(a.source_as_on_date) = b.full_date
 group by 3,4
 ) c
 on a.source_as_on_Date = c.Max_source_as_on_date and
 a.start_date=c.max_Start_date and
 month= c.month and
 year=c.year
 )
)select cast(a.month as date) as month,a.country_nm,a.region_nm,
b.fb_gain,b.fb_followers,
c.ig_gain,c.ig_followers,
d.yt_gain,d.yt_followers,
e.china_gain,e.china_followers ,
f.population,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_CP' AS etl_batch_id, 
		'bi_dbt_user_prd' as etl_insert_user_id,
		sysdate etl_insert_rec_dttm,
		'' etl_update_user_id,
		sysdate etl_update_rec_dttm 
from
( select distinct month,region_nm,country_nm from (
select month,region_nm,country_nm from __dbt__CTE__intm_cp_fb_followers_gain
union
select month,region_nm,country_nm from __dbt__CTE__intm_cp_ig_followers_gain
union
select cast(month as varchar) as month,region_nm,country_nm from __dbt__CTE__intm_cp_yt_followers_gain
union
select month,region_nm,country_nm from __dbt__CTE__intm_cp_china_followers_gain )
) a
left join __dbt__CTE__intm_cp_fb_followers_gain b
on a.month = b.month and a.country_nm=b.country_nm and a.region_nm=b.region_nm
left join __dbt__CTE__intm_cp_ig_followers_gain c
on a.month = c.month and a.country_nm = c.country_nm and a.region_nm = c.region_nm
left join __dbt__CTE__intm_cp_yt_followers_gain d
on a.month = d.month and a.country_nm = d.country_nm and a.region_nm = d.region_nm
left join __dbt__CTE__intm_cp_china_followers_gain e
on a.month = e.month and a.country_nm = e.country_nm and a.region_nm = e.region_nm
left join
(select case when country_name='USA' then 'United States' else country_name end as Country, sum(population) as Population 
from "entdwdb"."cdm"."dim_country_population" group by 1) f
on upper(a.country_nm)=upper(f.country)