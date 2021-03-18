


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