

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
),  __dbt__CTE__intm_cp_country_population as (


select dim_country_id,case when country_name='USA' then 'United States' else country_name end as Country, 
sum(population) as Population from cdm.dim_country_population group by 1,2
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
)select 
c.country,
a.date,
avg(c.population) as population,
sum(a.fb_gain) as fb_gain,
sum(a.fb_followers) as fb_followers,
sum(b.igm_gain) as igm_gain,
sum(b.igm_followers) as igm_followers,
sum(d.yt_gain) as yt_gain,
sum(d.yt_followers) as yt_followers
from
(select date,dim_country_id,fb_gain,fb_followers from __dbt__CTE__intm_cp_fb_followers_gain )a
 left join 
(select date,dim_country_id,igm_followers,igm_gain from __dbt__CTE__intm_cp_ig_followers_gain )b 
 on  a.date=b.date and a.dim_country_id=b.dim_country_id
 left join 
(select dim_country_id ,Country,Population from __dbt__CTE__intm_cp_country_population )c
 on  a.dim_country_id = c.dim_country_id
 left join
(select date,country,region,yt_gain,yt_followers from  __dbt__CTE__intm_cp_yt_followers_gain )d
 on  a.date = d.date and upper(c.country)=upper(d.country)
group by 1,2
order by 1,2