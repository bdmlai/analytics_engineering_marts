/***********************************************************************************/
/*                   tier 3 model - subscription history base                      */
/*                    db:analytics_workspace schema:content                        */
/* input tables: fds_nplus.fact_daily_subscription_status_plus*/
/* output table: jw_t3_vc_user_act_month */
/***********************************************************************************/
{{
    config(
        materialized='table'
    )
}}

/* getting all order history at order user level */
with

base as (
    
    select 
    
        *
    
    from {{ source('fds_nplus_prod', 'fact_daily_subscription_status_plus') }}
    {{ limit_in_dev ('exprd_entlmnt_date') }}

),

renamed as (

    select 
    
        src_fan_id, 
        order_id, 
        initial_order_date as start_date,
        exprd_entlmnt_date as end_date,
        to_date(to_date(to_char(current_date),'yyyy-mm-dd')) as today_date,   --convert to string so that script can be replaced by another date using python wrapper
        dateadd(mon,-3,today_date) as prev_3_month,   
        dateadd(mon,-12,today_date) as prev_12_month  
  
    from base
 
    qualify max(as_on_date) over(order by as_on_date desc) = as_on_date 
 
),

-- modify start and end date for each order by different time windows       
with_periods as (
    
    select 
    
        *,
       
        -- short-term
        case 
            when start_date < prev_3_month
                then prev_3_month
            else start_date 
        end as st_start,
                
        case 
            when end_date >= today_date
                then dateadd(d,-1,today_date)
            else end_date 
        end as st_end,
                
        -- mid-term
        case 
            when start_date < prev_12_month 
                then prev_12_month
            else start_date 
        end as mt_start,
        
        case 
            when end_date >= prev_3_month
                then dateadd(d,-1,prev_3_month)
            else end_date 
        end as mt_end,
        
        -- long-term
        case 
            when start_date < '2015-01-01' 
                then '2015-01-01'
            else start_date 
        end as lt_start,
        
        case 
            when end_date >= prev_12_month
                then dateadd(d,-1,prev_12_month)
            else end_date 
        end as lt_end,
        
        -- all_time
        case 
            when start_date < '2015-01-01' 
                then '2015-01-01'
            else start_date 
        end as all_time_start,
        
        case 
            when end_date >= today_date
                then dateadd(d,-1,today_date)
            else end_date 
        end as all_time_end,
        
        -- last_year
        case 
            when start_date < prev_12_month
                then prev_12_month
            else start_date 
        end as ly_start,
        
        case 
            when end_date >= today_date
                then dateadd(d,-1,today_date)
            else end_date 
        end as ly_end                     
 
    from renamed
    
),

-- calculate active months for each order for each time window
-- a month = 30 days
active_months as (
    
    select 
    
        *,
        -- short_term
        case 
            when datediff(d,st_start,st_end) < 0 
                then null
            else (datediff(d,st_start,st_end)+1.0)/30.0 
        end as st_act_mo,
        
        -- mid-term
        case 
            when datediff(d,mt_start,mt_end) < 0 
                then null
            else (datediff(d,mt_start,mt_end)+1.0)/30.0 
        end as mt_act_mo,
        
        -- long-term     
        case 
            when datediff(d,lt_start,lt_end) < 0 
                then null
            else (datediff(d,lt_start,lt_end)+1.0)/30.0 
        end as lt_act_mo,
        
        -- all_time     
        case 
            when datediff(d,all_time_start,all_time_end) < 0 
                then null
            else (datediff(d,all_time_start,all_time_end)+1.0)/30.0 
        end as all_time_act_mo,
        
        -- last year     
        case 
            when datediff(d,ly_start,ly_end) < 0 
                then null
            else (datediff(d,ly_start,ly_end)+1.0)/30.0 
        end as ly_act_mo
   
   from with_periods
   
),

-- group by user_id to calculate total active months for each time window
-- (some users may have placed several orders)
aggregated as (
    
    select 

        src_fan_id, 
        sum(st_act_mo) as st_act_mo,
        sum(mt_act_mo) as mt_act_mo,
        sum(lt_act_mo) as lt_act_mo,
        sum(all_time_act_mo) as all_time_act_mo,
        sum(ly_act_mo) as ly_act_mo,
        -- calculates max number of active months available since 2015-01-01
        datediff(mon,'2015-01-01',current_date) as max_months_available

    from active_months
    group by 1  
 
),
  
-- clean up, cap active months at max months available for each time window
-- some users may have multiple orders at the same time, inflating active months
cleaned as (

    select 

        src_fan_id,
        
        case 
            when st_act_mo > 3 
                then 3 
            else st_act_mo 
        end as st_act_mo_clean,
        
        case 
            when mt_act_mo > 9 
                then 9 
            else mt_act_mo 
        end as mt_act_mo_clean,
        
        case 
            when lt_act_mo > (max_months_available - 12) 
                then (max_months_available - 12) 
            else lt_act_mo 
        end as lt_act_mo_clean,
        
        case 
            when all_time_act_mo > max_months_available 
                then max_months_available 
            else all_time_act_mo 
        end as all_time_act_mo_clean,
        
        case 
            when ly_act_mo > 12 
                then 12 
            else ly_act_mo 
        end as ly_act_mo_clean

    from aggregated

) 

select * from cleaned

{# -- output table
drop table if exists al_t3_vc_user_act_month;
create table al_t3_vc_user_act_month as select * from user_act_month;
grant select on al_t3_vc_user_act_month to public;


select top 500* from al_t3_vc_user_act_month order by random();
 
select dateadd(mon,-3,current_date);
select date_trunc('month',current_date);
select to_date(to_char(current_date),'yyyy-mm-dd') as date_0,
dateadd(mon,-3,date_0) as _3mon

select top 100* from user_act_month_0;l #}