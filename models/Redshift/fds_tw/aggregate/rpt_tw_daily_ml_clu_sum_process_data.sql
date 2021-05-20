{{
  config({
		"schema": 'fds_tw',
		"materialized": 'table','tags': "rpt_tw_daily_ml_clu_sum_process_data" , "persist_docs": {'relation' : true, 'columns' : true},
	        'post-hook': 'grant select on {{ this }} to public'
                
  })
}}
select distinct a.show_date
        ,a.show_name
        ,a.segment_name        
        ,a.theme       
        ,b.tweet_cnt as tweet_count
        ,b.reaction_cnt as reaction_count
        ,b.speculation_cnt as speculation_count
        ,'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_content' as etl_batch_id
	,'bi_dbt_user_prd' as etl_insert_user_id
	, current_timestamp as etl_insert_rec_dttm
	, null as etl_update_user_id
	, cast(null as timestamp) as etl_update_rec_dttm

  from {{ref('intm_tw_process_data_split_by_theme')}} a
  join 
         (
                 select show_date
                        ,show_name
                        ,segment_name
                        ,theme
                        ,count(link) as tweet_cnt
                        ,sum(case when reaction_speculation='reaction' then 1 else 0 end ) as reaction_cnt
                        ,sum(case when reaction_speculation='speculation' then 1 else 0 end )as speculation_cnt
                 from {{ref('intm_tw_process_data_split_by_theme')}}
                 where theme <>'' and theme is not null
                 group by show_date,show_name,segment_name,theme
         ) b
    on a.show_date=b.show_date
   and a.show_name=b.show_name
   and a.segment_name=b.segment_name 
   and a.theme=b.theme
 where a.theme <>'' and a.theme is not null
          
 
 