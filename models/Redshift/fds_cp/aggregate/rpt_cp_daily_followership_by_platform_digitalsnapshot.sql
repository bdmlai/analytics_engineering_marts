{{
  config({
		'schema': 'fds_cp',
		"pre-hook": "truncate fds_cp.rpt_cp_daily_followership_by_platform_digitalsnapshot",
		"materialized": 'incremental','tags': "Content"
  })
}}
select month,country country_name,metric content_type,page,values as followers_count,platform,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_CP' AS etl_batch_id,
'bi_dbt_user_prd' as etl_insert_user_id,
sysdate etl_insert_rec_dttm,
'' etl_update_user_id,
sysdate etl_update_rec_dttm   from
(
select month,initcap(country) as country,metric,page,values,platform from {{ref('intm_cp_monthly_socialmedia_fb')}}
union all 
select month,initcap(country) as country,metric,page,values,platform from {{ref('intm_cp_monthly_socialmedia_igm')}}
union all
select cast(month as date),country,metric,page,cast(round(values) as int) as values,platform from {{ref('intm_cp_monthly_socialmedia_yt')}}
union all
select * from {{ref('intm_cp_monthly_youtube_combined')}}
union all
select * from {{ref('intm_cp_monthly_dotcom_combined')}}
)
