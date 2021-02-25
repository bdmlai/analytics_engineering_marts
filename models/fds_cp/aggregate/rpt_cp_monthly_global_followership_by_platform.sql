{{
  config({
		 'schema': 'fds_cp',	
		 "pre-hook": ["truncate fds_cp.rpt_cp_monthly_global_followership_by_platform"],
	     "materialized": 'incremental',"tags": 'cp',"persist_docs": {'relation' : true, 'columns' : true},
		 "post-hook": "drop table fds_cp.intm_youtube_subscribers_full_audiencecountries_unpivot"
        })
}}
select 
year,month,region_nm,country_nm,account_name,platform,followers,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_CP' AS etl_batch_id, 
		'bi_dbt_user_prd' as etl_insert_user_id,
		sysdate etl_insert_rec_dttm,
		'' etl_update_user_id,
		sysdate etl_update_rec_dttm 
 from
(
select year,month,region_nm,country_nm,account_name,platform_type as platform,followers from
{{ ref('intm_cp_fb_followers') }}
union all
select year,month,region_nm,country_nm,'N/A' as account_name,platform_type as platform,followers from
{{ ref('intm_cp_ig_followers') }}
union all
select year,month,'GlOBAL' as region_nm,'GLOBAL' AS country_nm, account_name, platform,followers from
{{ ref('intm_cp_tw_followers') }}
union all
select year,month, region_nm,country_nm, account_name, platform,followers :: bigint from
{{ ref('intm_cp_yt_followers') }}
union all
select year,month, region_nm,country_nm, 'N/A' as account_name, platform,followers from
{{ ref('intm_cp_china_social_followers') }}

)