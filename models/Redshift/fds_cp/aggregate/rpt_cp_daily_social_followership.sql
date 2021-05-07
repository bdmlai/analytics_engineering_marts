{{
  config({
		'schema': 'fds_cp',
		"pre-hook": "truncate fds_cp.rpt_cp_daily_social_followership",
		"materialized": 'incremental','tags': "Content","persist_docs": {'relation' : true, 'columns' : true},
		"post-hook" : 'grant select on {{this}} to public'
  })
}}


select cast(a.month as date) as month,a.country_nm,a.region_nm,
b.fb_gain,b.fb_followers,
c.ig_gain,c.ig_followers,
d.yt_gain,d.yt_followers,
e.china_gain,e.china_followers ,
f.population,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_CP' AS etl_batch_id, 
		'bi_dbt_user_prd' as etl_insert_user_id,
		sysdate etl_insert_rec_dttm,
		'' etl_update_user_id,
		sysdate etl_update_rec_dttm 
from
( select distinct month,region_nm,country_nm from (
select month,region_nm,country_nm from {{ref('intm_cp_fb_followers_gain')}}
union
select month,region_nm,country_nm from {{ref('intm_cp_ig_followers_gain')}}
union
select cast(month as varchar) as month,region_nm,country_nm from {{ref('intm_cp_yt_followers_gain')}}
union
select month,region_nm,country_nm from {{ref('intm_cp_china_followers_gain')}} )
) a
left join {{ref('intm_cp_fb_followers_gain')}} b
on a.month = b.month and a.country_nm=b.country_nm and a.region_nm=b.region_nm
left join {{ref('intm_cp_ig_followers_gain')}} c
on a.month = c.month and a.country_nm = c.country_nm and a.region_nm = c.region_nm
left join {{ref('intm_cp_yt_followers_gain')}} d
on a.month = d.month and a.country_nm = d.country_nm and a.region_nm = d.region_nm
left join {{ref('intm_cp_china_followers_gain')}} e
on a.month = e.month and a.country_nm = e.country_nm and a.region_nm = e.region_nm
left join
(select case when country_name='USA' then 'United States' else country_name end as Country, sum(population) as Population 
from {{source('cdm','dim_country_population')}} group by 1) f
on upper(a.country_nm)=upper(f.country)
