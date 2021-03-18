
with __dbt__CTE__intm_cp_fb_followers as (



select b.cal_year as year,b.cal_mth_num as month,
nvl(f.region_nm,'Other') as region_nm,
nvl(regexp_replace(INITCAP(k.country_nm),'[^0-9A-z ,\\.()\\-]',''),'Other') as country_nm,
d.account_name, g.platform_type,sum(c_followers) as followers
from "entdwdb"."fds_fbk"."fact_fb_smfollowership_audience_bycountry"  a  left join
"entdwdb"."cdm"."dim_smprovider_account" d on a.dim_Smprovider_account_id = d.dim_smprovider_account_id
left join ( select distinct dim_country_id,region_nm from "entdwdb"."cdm"."dim_region_country"  where ent_map_nm = 'GM Region')f
on a.dim_country_id = f.dim_Country_id
left join ( select distinct dim_country_id,src_country_name as country_nm from "entdwdb"."cdm"."dim_country"  ) k
on a.dim_country_id = k.dim_Country_id
 left join "entdwdb"."cdm"."dim_platform" g on a.dim_platform_id=g.dim_platform_id left join
"entdwdb"."cdm"."dim_date" b on a.dim_date_id = b.dim_date_id
 join
(
select b.cal_year as year,b.cal_mth_num as month,
dim_country_id,d.account_name,max(a.dim_date_id) as last_day from
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
group by 1,2,3,4,5,6
),  __dbt__CTE__intm_cp_ig_followers as (


select year,month,region_nm,country_nm,platform_type,sum(followers) as followers from (
select b.cal_year as year,b.cal_mth_num as month,
nvl(f.region_nm,'Other') as region_nm,
nvl(regexp_replace(INITCAP(k.country_nm),'[^0-9A-z ,\\.()\\-]',''),'Other') as country_nm,
d.account_name,
 g.platform_type,(c_followers) as followers
 from "entdwdb"."fds_igm"."fact_ig_smfollowership_audience_bycountry" a left join
"entdwdb"."cdm"."dim_smprovider_account" d on a.dim_Smprovider_account_id = d.dim_smprovider_account_id
left join ( select distinct dim_country_id,region_nm from "entdwdb"."cdm"."dim_region_country"  where ent_map_nm = 'GM Region' )f
 on a.dim_country_id = f.dim_Country_id
 left join ( select distinct dim_country_id,src_country_name as country_nm from "entdwdb"."cdm"."dim_country"  ) k
on a.dim_country_id = k.dim_Country_id
left join "entdwdb"."cdm"."dim_platform" g on a.dim_platform_id=g.dim_platform_id left join
"entdwdb"."cdm"."dim_date" b on a.dim_date_id = b.dim_date_id
 join
(
select b.cal_year as year,b.cal_mth_num as month,
dim_country_id,d.account_name,max(a.dim_date_id) as last_day from
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
group by 1,2,3,4,5
),  __dbt__CTE__intm_cp_tw_followers as (


select b.cal_year as year,b.cal_mth_num as month,
d.account_name, 'TW' as platform,
(a.twitter_followers)  as followers
from "entdwdb"."fds_cp"."fact_co_smfollowership_cumulative_summary"  a left join
"entdwdb"."cdm"."dim_smprovider_account" d on a.dim_Smprovider_account_id = d.dim_smprovider_account_id  left join
"entdwdb"."cdm"."dim_date" b on a.dim_date_id = b.dim_date_id
 join
(
select b.cal_year as year,b.cal_mth_num as month,
d.account_name,max(a.dim_date_id) as last_day 
from "entdwdb"."fds_cp"."fact_co_smfollowership_cumulative_summary"   a left join
"entdwdb"."cdm"."dim_smprovider_account" d on a.dim_Smprovider_account_id = d.dim_smprovider_account_id left join
"entdwdb"."cdm"."dim_date" b on a.dim_date_id = b.dim_date_id

group by 1,2,3
) h 
on month=h.month and 
year=h.year and
d.account_name=h.account_name
and a.dim_date_id=h.last_day
),  __dbt__CTE__intm_youtube_subscribers_full_audiencecountries_unpivot as (

with base as
(

    )
select * from base
union all
 select
        account_name,
        as_on_date,
      cast('as' as varchar) as country_cd,
      cast("as" as varchar) as subscriber_count
    from "entdwdb"."fds_cp"."intm_youtube_subscribers_full_audiencecountries"
    union all  
	    select
        account_name,
        as_on_date,
      cast('do' as varchar) as country_cd,
      cast("do" as varchar) as subscriber_count
    from "entdwdb"."fds_cp"."intm_youtube_subscribers_full_audiencecountries"
    union all
	    select
        account_name,
        as_on_date,
      cast('in' as varchar) as country_cd,
      cast("in" as varchar) as subscriber_count
    from "entdwdb"."fds_cp"."intm_youtube_subscribers_full_audiencecountries"
    union all
    select
        account_name,
        as_on_date,
      cast('is' as varchar) as country_cd,
      cast("is" as varchar) as subscriber_count
    from "entdwdb"."fds_cp"."intm_youtube_subscribers_full_audiencecountries"
	union all
    select
        account_name,
        as_on_date,
      cast('to' as varchar) as country_cd,
      cast("to" as varchar) as subscriber_count
    from "entdwdb"."fds_cp"."intm_youtube_subscribers_full_audiencecountries"
),  __dbt__CTE__intm_cp_yt_followers as (


select  b.cal_year as year,b.cal_mth_num as month,
nvl(f.region_nm,'Other') as region_nm,
nvl(regexp_replace(INITCAP(k.country_nm),'[^0-9A-z ,\\.()\\-]',''),'Other') as country_nm,
a.account_name, 'YT' as platform,subscriber_count as followers
from __dbt__CTE__intm_youtube_subscribers_full_audiencecountries_unpivot a
left join ( select distinct iso_alpha2_ctry_cd,region_nm from "entdwdb"."cdm"."dim_region_country" where ent_map_nm = 'GM Region'  ) f
on a.country_cd = f.iso_alpha2_ctry_cd 
left join ( select  iso_alpha2_ctry_cd,src_country_name as country_nm from "entdwdb"."cdm"."dim_country" where src_sys_Cd='iso' ) k
on a.country_cd = k.iso_alpha2_ctry_cd 
left join
"entdwdb"."cdm"."dim_date" b on a.as_on_date = b.dim_date_id
 join
(
select  b.cal_year as year,b.cal_mth_num as month,
country_cd,account_name,max(a.as_on_date) as last_day from
__dbt__CTE__intm_youtube_subscribers_full_audiencecountries_unpivot a left join
"entdwdb"."cdm"."dim_date" b on a.as_on_date = b.dim_date_id
group by 1,2,3,4
) h 
on MONTH=h.MONTH and 
YEAR=h.year and
f.iso_alpha2_ctry_cd=h.country_cd and
a.account_name=h.account_name
and a.as_on_date=h.last_day
),  __dbt__CTE__intm_cp_china_social_followers as (


select  b.cal_year as year,b.cal_mth_num as month,
'China_social' as Platform,
'CHINA' as country_nm, 'APAC' AS region_nm,
(weibo_total_followers +
wechat_total_wechat_followers  +
 wechat_total_qzone_followers  +
toutiao_total_followers +
 wechat_total_vplus_subscribers   +
 youku_total_followers) as followers from 
 "entdwdb"."hive_udl_chscl"."china_weekly_social_data" a left join
"entdwdb"."cdm"."dim_date" b on trunc(a.source_as_on_date) = b.full_date
 join
 (
Select max(source_as_on_date) as Max_source_as_on_date,
max(Start_date) as max_start_Date,
b.cal_year as year,b.cal_mth_num as month
 from "entdwdb"."hive_udl_chscl"."china_weekly_social_data" a left join
"entdwdb"."cdm"."dim_date" b on trunc(a.source_as_on_date) = b.full_date
 group by 3,4
 ) c
 on a.source_as_on_Date = c.Max_source_as_on_date and
 a.start_date=c.max_Start_date and
 month= c.month and
 year=c.year
)select 
year,month,region_nm,country_nm,account_name,platform,followers,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_CP' AS etl_batch_id, 
		'bi_dbt_user_prd' as etl_insert_user_id,
		sysdate etl_insert_rec_dttm,
		'' etl_update_user_id,
		sysdate etl_update_rec_dttm 
 from
(
select year,month,region_nm,country_nm,account_name,platform_type as platform,followers from
__dbt__CTE__intm_cp_fb_followers
union all
select year,month,region_nm,country_nm,'N/A' as account_name,platform_type as platform,followers from
__dbt__CTE__intm_cp_ig_followers
union all
select year,month,'GlOBAL' as region_nm,'GLOBAL' AS country_nm, account_name, platform,followers from
__dbt__CTE__intm_cp_tw_followers
union all
select year,month, region_nm,country_nm, account_name, platform,followers :: bigint from
__dbt__CTE__intm_cp_yt_followers
union all
select year,month, region_nm,country_nm, 'N/A' as account_name, platform,followers from
__dbt__CTE__intm_cp_china_social_followers

)