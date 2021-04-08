/***********************************************************************************/
/*                  8.  tier 3 model - final consolidated output                   */
/* db:analytics_workspace schema:content                                           */
/* input tables: intm_nplus_viewership_cluster_clustering_ly                       */
/*               intm_nplus_viewership_cluster_clustering_st                       */
/*               intm_nplus_viewership_cluster_clustering_mt                       */
/*               intm_nplus_viewership_cluster_clustering_lt                       */
/*               intm_nplus_viewership_cluster_clustering_all_time                 */
/* ouput tables: rpt_nplus_viewership_cluster_clustering                          */
/***********************************************************************************/
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key= 'unique_id',
        tags=['viewership','viewership_model'],
		schema='fds_nplus',
		post_hook=["drop table dt_stage.intm_nplus_viewership_cluster_available_content;  
					  drop table dt_stage.intm_nplus_viewership_cluster_content_duration_all_time;
					  drop table dt_stage.intm_nplus_viewership_cluster_clustering_all_time;           
					  drop table dt_stage.intm_nplus_viewership_cluster_content_duration_lt;
					  drop table dt_stage.intm_nplus_viewership_cluster_clustering_lt;           
					  drop table dt_stage.intm_nplus_viewership_cluster_content_duration_ly;
					  drop table dt_stage.intm_nplus_viewership_cluster_clustering_ly;           
					  drop table dt_stage.intm_nplus_viewership_cluster_content_duration_mt;
					  drop table dt_stage.intm_nplus_viewership_cluster_clustering_mt;           
					  drop table dt_stage.intm_nplus_viewership_cluster_content_duration_st;
					  drop table dt_stage.intm_nplus_viewership_cluster_clustering_st;                
					  drop table dt_stage.intm_nplus_viewership_cluster_user_act_month;
					  drop table dt_stage.intm_nplus_viewership_cluster_clustering_sub_rules_all_time;
					  drop table dt_stage.intm_nplus_viewership_cluster_user_consumption_all_time;
					  drop table dt_stage.intm_nplus_viewership_cluster_clustering_sub_rules_lt;
					  drop table dt_stage.intm_nplus_viewership_cluster_user_consumption_lt;
					  drop table dt_stage.intm_nplus_viewership_cluster_clustering_sub_rules_ly;
					  drop table dt_stage.intm_nplus_viewership_cluster_user_consumption_ly;
					  drop table dt_stage.intm_nplus_viewership_cluster_clustering_sub_rules_mt;
					  drop table dt_stage.intm_nplus_viewership_cluster_user_consumption_mt;
					  drop table dt_stage.intm_nplus_viewership_cluster_clustering_sub_rules_st;
					  drop table dt_stage.intm_nplus_viewership_cluster_user_consumption_st;
					  drop table dt_stage.intm_nplus_viewership_cluster_content_cluster;"]
    )
}}

with 

ly as (
    
    select * from {{ ref('intm_nplus_viewership_cluster_clustering_ly') }}
    
),

st as (
    
    select * from {{ ref('intm_nplus_viewership_cluster_clustering_st') }}
    
),


mt as (
    
    select * from {{ ref('intm_nplus_viewership_cluster_clustering_mt') }}
    
),

lt as (
    
    select * from {{ ref('intm_nplus_viewership_cluster_clustering_lt') }}
    
),


all_time as (
    
    select * from {{ ref('intm_nplus_viewership_cluster_clustering_all_time') }}
    
)

select *,'DBT_'||TO_CHAR(SYSDATE(),'YYYY_MM_DD_HH_MI_SS')||'_viewership' as etl_batch_id
	, 'bi_dbt_user_prd' as etl_insert_user_id
	, current_timestamp as etl_insert_rec_dttm
	, null as etl_update_user_id
	, cast(null as timestamp) as etl_update_rec_dttm from 
((select *,
        {{  dbt_utils.surrogate_key(['as_on_date','model_name','src_fan_id']) }} as unique_id
   from ly)
union
(select * ,
											  
        {{  dbt_utils.surrogate_key(['as_on_date','model_name','src_fan_id']) }} as unique_id 
   from st)
union
(select *,
											  
        {{  dbt_utils.surrogate_key(['as_on_date','model_name','src_fan_id']) }} as unique_id  
   from mt)
union
(select *,
											  
        {{  dbt_utils.surrogate_key(['as_on_date','model_name','src_fan_id']) }} as unique_id 
   from lt)
union
(select *,
											  
        {{  dbt_utils.surrogate_key(['as_on_date','model_name','src_fan_id']) }} as unique_id 
   from all_time))
