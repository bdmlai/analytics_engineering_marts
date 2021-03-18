

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