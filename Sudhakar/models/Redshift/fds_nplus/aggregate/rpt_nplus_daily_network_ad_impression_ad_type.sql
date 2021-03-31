{{
  config({
		'schema': 'fds_nplus',
		"materialized": 'table','tags': "rpt_nplus_daily_network_ad_impression_ad_type","persist_docs": {'relation' : true, 'columns' : true} ,
                "post-hook" : 'grant select on {{this}} to public'
  })
}}


            SELECT start_date, start_time, content_id, content_description,
            duration, end_time, ad_abbreviation, ad_category, ad_type, audience_type, 
	    CAST(replace(concurrent_plays, ',', '') as numeric) as concurrent_plays,
'DBT_'+To_char(sysdate,'YYYY_MM_DD_HH_MI_SS')+'_NPLUS' AS etl_batch_id,
       'bi_dbt_user_prd' AS etl_insert_user_id,
       sysdate etl_insert_rec_dttm,
       '' etl_update_user_id,
       sysdate etl_update_rec_dttm
            FROM {{ref('intm_nplus_aa_network_ad_impressions_jl_step1')}}