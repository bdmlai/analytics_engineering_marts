{{
  config({
		"materialized": 'ephemeral'
  })
}}

select year,month,region_nm,country_nm,platform_type,sum(followers) as followers from (
select b.cal_year as year,b.cal_mth_num as month,
f.region_nm,f.country_nm,d.account_name,
 g.platform_type,(c_followers) as followers
 from {{source('fds_igm','fact_ig_smfollowership_audience_bycountry')}} a left join
{{source('cdm','dim_smprovider_account')}} d on a.dim_Smprovider_account_id = d.dim_smprovider_account_id
left join ( select distinct country_nm,dim_country_id,region_nm from {{source('cdm','dim_region_country')}} )f
 on a.dim_country_id = f.dim_Country_id
left join {{source('cdm','dim_platform')}} g on a.dim_platform_id=g.dim_platform_id left join
{{source('cdm','dim_date')}} b on a.dim_date_id = b.dim_date_id
 join
(
select b.cal_year as year,b.cal_mth_num as month,
dim_country_id,d.account_name,max(a.dim_date_id) as last_day from
{{source('fds_igm','fact_ig_smfollowership_audience_bycountry')}} a left join
{{source('cdm','dim_smprovider_account')}} d on a.dim_Smprovider_account_id = d.dim_smprovider_account_id left join
{{source('cdm','dim_date')}} b on a.dim_date_id = b.dim_date_id
group by 1,2,3,4
) h 
on month=h.month and 
year=h.year and
a.dim_country_id=h.dim_country_id  and
d.account_name=h.account_name
and a.dim_date_id=h.last_day
where b.cal_year_mth_num > (select  nvl(max(convert(varchar,year)|| case when len(month)=1 then '0'||convert(varchar,month) 
else convert(varchar,month) end),'190001')
from fds_cp.rpt_cp_monthly_global_followership_by_platform where platform = 'IG')
)
group by 1,2,3,4,5