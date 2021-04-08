/***********************************************************************************/
/*                 3. tier 3 model - available content duration base               */
/* db:analytics_workspace schema:content                                           */
/* input tables: fds_nplus.fact_daily_subscription_status_plus                     */
/*               intm_nplus_viewership_cluster_content_cluster                                     */
/* ouput tables: intm_nplus_viewership_cluster_available_content                                   */
/***********************************************************************************/
{{
    config(
        materialized='table',
        tags=['viewership','viewership_base'],
		schema='dt_stage'
    )
}}

with

base as (
    
    select         src_fan_id, 
        order_id, 
        initial_order_date as start_date,
        exprd_entlmnt_date as end_date 
    from {{ source('fds_nplus_prod', 'fact_daily_subscription_status_plus') }}
    where as_on_date in (select max(as_on_date) from {{ source('fds_nplus_prod', 'fact_daily_subscription_status_plus') }})
),

cluster as (
    
    select * from {{ ref('intm_nplus_viewership_cluster_content_cluster') }}
    
),

/* getting all order history at order user level */
orders as (
    
    select 
        *,
        to_date(to_date(to_char(ifnull(nullif('{{var("as_on_date")}}','None'),date_trunc(month,current_date)-1)),'yyyy-mm-dd')) as as_on_date,   --convert to string so that script can be replaced by another date using python wrapper
        dateadd(mon,-3,as_on_date) as prev_3_month,   
        dateadd(mon,-12,as_on_date) as prev_12_month
    
    from base
    
    -- WHAT IS THE INTENT OF THIS?
    qualify max(as_on_date) over(order by as_on_date desc) = as_on_date 
    
),

/* 
select count(*) from orders; --14,478,550
select count(*) from orders where end_date >= '2019-01-01'  --5,433,245
select top 100* from orders;   -- src_fan_id
select top 100* from al_t3_vc_content_cluster; */

-- find assets premiering during users' active period (deduped)
available_assets as (
    
    select 
    
        production_id,
        content_duration,
        content_class,
        any_network_first_stream,
        first_stream as network_first_stream,
        row_number() over
            (
                partition by 
                    production_id,
                    content_class 
                order by content_duration desc nulls last
            ) as row_num
    
    from cluster
    qualify row_num = 1
      and production_id is not null
      and content_duration is not null
      and content_class in 
            ('NXT TakeOver','Tier 1 PPV','Tier 2 PPV',     --big events
             '205 Live','In-Ring Tournament','NXT','NXT UK','In-Ring Special'  -- network in-ring
            )    
),

--code to place things in hourly buckets
bracketed as (
    
    select 
    
        orders.*,
        available_assets.production_id,
        available_assets.content_duration,
        available_assets.content_class,
        available_assets.any_network_first_stream,
        to_date(available_assets.network_first_stream) as network_first_stream

    from orders
  
    inner join available_assets 
        on available_assets.network_first_stream > orders.start_date - 7   -- give 7 day buffer
       and available_assets.network_first_stream < orders.end_date + 1     -- give 1 day buffer
   
),

-- dedupe
-- duplicates may happen when a user have multiple orders at the same time
final as (
    
    select distinct 
    
        src_fan_id, 
        order_id, 
        start_date,
        end_date, 
        as_on_date, 
        prev_3_month, 
        prev_12_month,
        production_id,
        content_duration,
        content_class,
        any_network_first_stream,
        network_first_stream
    
    from bracketed

),

with_id as (
    
    select 
    
        *,
        {{  dbt_utils.surrogate_key(['src_fan_id','production_id','order_id']) }}
            as unique_id
        
    from final
    
)

select * from with_id

 