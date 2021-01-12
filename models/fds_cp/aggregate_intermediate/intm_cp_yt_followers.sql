{{
    config({
        "materialized": 'ephemeral'

         })
			
}}

select  b.cal_year as year,b.cal_mth_num as month,
f.region_nm,f.country_nm,
a.account_name, 'YT' as platform,subscriber_count as followers
from {{ ref('intm_youtube_subscribers_full_audiencecountries_unpivot') }} a
left join ( select distinct country_nm,iso_alpha2_ctry_cd,region_nm from cdm.dim_region_country )f
on a.country_cd = f.iso_alpha2_ctry_cd left join
{{source('cdm','dim_date')}} b on a.as_on_date = b.dim_date_id
 join
(
select  b.cal_year as year,b.cal_mth_num as month,
country_cd,account_name,max(a.as_on_date) as last_day from
{{ ref('intm_youtube_subscribers_full_audiencecountries_unpivot') }} a left join
{{source('cdm','dim_date')}} b on a.as_on_date = b.dim_date_id
group by 1,2,3,4
) h 
on MONTH=h.MONTH and 
YEAR=h.year and
f.iso_alpha2_ctry_cd=h.country_cd and
a.account_name=h.account_name
and a.as_on_date=h.last_day
where b.cal_year_mth_num > (select  nvl(max (convert(varchar,year)|| case when len(month)=1 then '0'||convert(varchar,month) 
else convert(varchar,month) end),'190001')
from fds_cp.rpt_cp_monthly_global_followership_by_platform where platform ='YT')