{{
  config({
		"materialized": 'ephemeral'
  })
}}

select a.month, coalesce(b.country_nm, 'unknown') as country_nm, coalesce(b.region_nm, 'unknown') as region_nm, 
sum(a.total_active_cnt) as active_subs
from
(select date_trunc('month',as_on_date-1) as month, country_cd,
sum(total_active_cnt) as total_active_cnt
from {{source('fds_nplus','AGGR_TOTAL_SUBSCRIPTION')}}
where as_on_date = date_trunc('month',as_on_date) and as_on_date> '2018-01-01'
group by 1,2)a
left join
(select region_nm,country_nm,dim_country_id, iso_alpha2_ctry_cd from {{source('cdm','dim_region_country')}}
 where ent_map_nm = 'GM Region')b
-- Adding upper() on joining condition to remove nulls coming in hours_Watched metric jira - PSTA-1905
on upper(a.country_cd)=upper(b.iso_alpha2_ctry_cd)
group by 1,2,3