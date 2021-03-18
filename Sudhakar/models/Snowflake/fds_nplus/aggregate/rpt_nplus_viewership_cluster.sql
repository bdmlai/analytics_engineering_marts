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
		post_hook=["drop table fds_nplus.intm_nplus_viewership_cluster_available_content;  
					  drop table fds_nplus.intm_nplus_viewership_cluster_content_duration_all_time;
					  drop table fds_nplus.intm_nplus_viewership_cluster_clustering_all_time;           
					  drop table fds_nplus.intm_nplus_viewership_cluster_content_duration_lt;
					  drop table fds_nplus.intm_nplus_viewership_cluster_clustering_lt;           
					  drop table fds_nplus.intm_nplus_viewership_cluster_content_duration_ly;
					  drop table fds_nplus.intm_nplus_viewership_cluster_clustering_ly;           
					  drop table fds_nplus.intm_nplus_viewership_cluster_content_duration_mt;
					  drop table fds_nplus.intm_nplus_viewership_cluster_clustering_mt;           
					  drop table fds_nplus.intm_nplus_viewership_cluster_content_duration_st;
					  drop table fds_nplus.intm_nplus_viewership_cluster_clustering_st;                
					  drop table fds_nplus.intm_nplus_viewership_cluster_user_act_month;
					  drop table fds_nplus.intm_nplus_viewership_cluster_clustering_sub_rules_all_time;
					  drop table fds_nplus.intm_nplus_viewership_cluster_user_consumption_all_time;
					  drop table fds_nplus.intm_nplus_viewership_cluster_clustering_sub_rules_lt;
					  drop table fds_nplus.intm_nplus_viewership_cluster_user_consumption_lt;
					  drop table fds_nplus.intm_nplus_viewership_cluster_clustering_sub_rules_ly;
					  drop table fds_nplus.intm_nplus_viewership_cluster_user_consumption_ly;
					  drop table fds_nplus.intm_nplus_viewership_cluster_clustering_sub_rules_mt;
					  drop table fds_nplus.intm_nplus_viewership_cluster_user_consumption_mt;
					  drop table fds_nplus.intm_nplus_viewership_cluster_clustering_sub_rules_st;
					  drop table fds_nplus.intm_nplus_viewership_cluster_user_consumption_st;
					  drop table fds_nplus.intm_nplus_viewership_cluster_content_cluster;"]
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

(select *, 
        CURRENT_TIMESTAMP(0) as code_run_time, 
        {{  dbt_utils.surrogate_key(['as_on_date','model_name','src_fan_id']) }} as unique_id
   from ly)
union
(select *, 
        CURRENT_TIMESTAMP(0) as code_run_time,
        {{  dbt_utils.surrogate_key(['as_on_date','model_name','src_fan_id']) }} as unique_id 
   from st)
union
(select *, 
        CURRENT_TIMESTAMP(0) as code_run_time,
        {{  dbt_utils.surrogate_key(['as_on_date','model_name','src_fan_id']) }} as unique_id  
   from mt)
union
(select *, 
        CURRENT_TIMESTAMP(0) as code_run_time,
        {{  dbt_utils.surrogate_key(['as_on_date','model_name','src_fan_id']) }} as unique_id 
   from lt)
union
(select *, 
        CURRENT_TIMESTAMP(0) as code_run_time,
        {{  dbt_utils.surrogate_key(['as_on_date','model_name','src_fan_id']) }} as unique_id 
   from all_time)
