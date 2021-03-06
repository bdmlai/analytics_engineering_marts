{{
  config({
		"materialized": 'ephemeral'
  })
}}

select year||'-'||case when length(month) =1 then '0'||(month :: varchar(2)) else( month :: varchar(2)) end ||'-01' as month,region_nm,
regexp_replace(initcap(country_nm),'[^0-9A-z ,\\.()\\-]','') as country_nm,sum(gain) as fb_gain,sum(followers) as fb_followers from
(
select b.cal_year as year,b.cal_mth_num as month,
nvl(f.region_nm,'Other') as region_nm,nvl(f.country_nm,'Other') as country_nm,
d.account_name,(c_followers) as followers ,(h.dod_followers) as gain
from {{source('fds_fbk','fact_fb_smfollowership_audience_bycountry')}}  a  left join
{{source('cdm','dim_smprovider_account')}} d on a.dim_Smprovider_account_id = d.dim_smprovider_account_id
left join ( select distinct country_nm,dim_country_id,region_nm from {{source('cdm','dim_region_country')}} where ent_map_nm = 'GM Region' )f
on a.dim_country_id = f.dim_Country_id left join
{{source('cdm','dim_date')}} b on a.dim_date_id = b.dim_date_id
 join
(
select b.cal_year as year,b.cal_mth_num as month,
dim_country_id,d.account_name,sum(dod_followers) as dod_followers ,max(a.dim_date_id) as last_day from
{{source('fds_fbk','fact_fb_smfollowership_audience_bycountry')}}  a  left join
{{source('cdm','dim_smprovider_account')}} d on a.dim_Smprovider_account_id = d.dim_smprovider_account_id left join
{{source('cdm','dim_date')}} b on a.dim_date_id = b.dim_date_id
group by 1,2,3,4
) h 
on MONTH=h.month and 
YEAR=h.year and
a.dim_country_id=h.dim_country_id and
d.account_name=h.account_name
and a.dim_date_id=h.last_day
)
group by 1,2,3