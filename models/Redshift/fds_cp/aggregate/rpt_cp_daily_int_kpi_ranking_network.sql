{{
  config({
		'schema': 'fds_cp',
		"pre-hook": "truncate fds_cp.rpt_cp_daily_int_kpi_ranking_network",
		"materialized": 'incremental','tags': "Content","persist_docs": {'relation' : true, 'columns' : true},
		"post-hook" : 'grant select on {{this}} to public'
  })
}}



select cast(a.month as date) as month, a.country_nm, a.region_nm, b.active_subs, c.hours_watched,c.views, d.population,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_CP' AS etl_batch_id, 
		'bi_dbt_user_prd' as etl_insert_user_id,
		sysdate etl_insert_rec_dttm,
		'' etl_update_user_id,
		sysdate etl_update_rec_dttm 
 from
(select distinct month, country_nm, region_nm from
(select month, country_nm, region_nm from {{ref('intm_cp_network_subscribers')}}
union
select month, country_nm, region_nm from {{ref('intm_cp_network_hours')}})
) a
left join {{ref('intm_cp_network_subscribers')}} b
on trunc(a.month) = trunc(b.month) and a.country_nm = b.country_nm and a.region_nm = b.region_nm
left join {{ref('intm_cp_network_hours')}} c
on trunc(a.month) = trunc(c.month) and a.country_nm = c.country_nm and a.region_nm = c.region_nm
left join
(select case when country_name='USA' then 'United States' else country_name end as Country, sum(population) as Population 
from {{source('cdm','dim_country_population')}} group by 1) d
on upper(a.country_nm)=upper(d.country)
