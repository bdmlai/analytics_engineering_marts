{{
  config({
		"schema": 'fds_yt',
		"pre-hook": ["truncate fds_yt.rpt_yt_daily_wwe_talent_views_pastyear"],
		"materialized": 'incremental','tags': "Content", "persist_docs": {'relation' : true, 'columns' : true},
		'post-hook': 'grant select on fds_yt.rpt_yt_daily_wwe_talent_views_pastyear to public'

  })
}}

select region_code,initcap(country_nm) as region_name, talent, 'past year' as granularity,
sum(ttl_views) as total_views, sum(views_30days) as views_30days, 
count(distinct video_id) as cnt_video_id,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_content' as etl_batch_id,
 'bi_dbt_user_prd' as etl_insert_user_id, 
current_timestamp as etl_insert_rec_dttm, 
null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm

from {{ref('intm_yt_daily_wwe_talent_pastyear')}} a
inner join {{source('cdm','dim_region_country')}}  b on a.region_code=upper(b.iso_alpha2_ctry_cd)
where b.ent_map_nm ='GM Region'
group by 1,2,3
order by 1 desc, 2,3 asc  