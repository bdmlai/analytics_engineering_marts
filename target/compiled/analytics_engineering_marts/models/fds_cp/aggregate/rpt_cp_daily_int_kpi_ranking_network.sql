



with __dbt__CTE__intm_cp_network_subscribers as (


select a.month, coalesce(b.country_nm, 'unknown') as country_nm, coalesce(b.region_nm, 'unknown') as region_nm, 
sum(a.total_active_cnt) as active_subs
from
(select date_trunc('month',as_on_date-1) as month, country_cd,
sum(total_active_cnt) as total_active_cnt
from "entdwdb"."fds_nplus"."AGGR_TOTAL_SUBSCRIPTION"
where as_on_date = date_trunc('month',as_on_date) and as_on_date> '2018-01-01'
group by 1,2)a
left join
(select region_nm,country_nm,dim_country_id, iso_alpha2_ctry_cd from "entdwdb"."cdm"."dim_region_country"
 where ent_map_nm = 'GM Region')b
-- Adding upper() on joining condition to remove nulls coming in hours_Watched metric jira - PSTA-1905
on upper(a.country_cd)=upper(b.iso_alpha2_ctry_cd)
group by 1,2,3
),  __dbt__CTE__intm_cp_network_hours as (




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
)select cast(a.month as date) as month, a.country_nm, a.region_nm, b.active_subs, c.hours_watched,c.views, d.population,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_CP' AS etl_batch_id, 
		'bi_dbt_user_prd' as etl_insert_user_id,
		sysdate etl_insert_rec_dttm,
		'' etl_update_user_id,
		sysdate etl_update_rec_dttm 
 from
(select distinct month, country_nm, region_nm from
(select month, country_nm, region_nm from __dbt__CTE__intm_cp_network_subscribers
union
select month, country_nm, region_nm from __dbt__CTE__intm_cp_network_hours)
) a
left join __dbt__CTE__intm_cp_network_subscribers b
on trunc(a.month) = trunc(b.month) and a.country_nm = b.country_nm and a.region_nm = b.region_nm
left join __dbt__CTE__intm_cp_network_hours c
on trunc(a.month) = trunc(c.month) and a.country_nm = c.country_nm and a.region_nm = c.region_nm
left join
(select case when country_name='USA' then 'United States' else country_name end as Country, sum(population) as Population 
from "entdwdb"."cdm"."dim_country_population" group by 1) d
on upper(a.country_nm)=upper(d.country)