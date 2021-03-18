



select a.month, coalesce(b.country_nm, 'unknown') as country_nm, coalesce(b.region_nm, 'unknown') as region_nm, 
sum(a.hours_watched) as hours_watched , sum(a.views)  as views
from
(select a.mnth_start_dt as month,
Case when c.gbl_country_name is NULL then 'Unknown' else initcap(c.gbl_country_name) End as country,
sum(a.mnthly_hours_consumed) as hours_watched,
-- Added views as part of enhancement on network KPI ranking . Jira - PSTA-1897
sum(a.mnthly_view_cnt) as views
from
"entdwdb"."fds_nplus"."AGGR_MONTHLY_DVC_PLATFORM_CNTRY_VIEWERSHIP" a
join "entdwdb"."cdm"."dim_country" as c on a.dim_country_id = c.dim_country_id
where dim_stream_type_id = 4 and subs_tier = '95' and mnth_start_dt >='2018-01-01' 
group by 1,2) a
left join
(select region_nm, country_nm, dim_country_id, iso_alpha2_ctry_cd from "entdwdb"."cdm"."dim_region_country"
 where ent_map_nm = 'GM Region') b
-- Adding upper() in joining condition to remove nulls coming in metric - jira -PSTA-1905 .
on upper(a.country)=upper(b.country_nm)
group by 1,2,3