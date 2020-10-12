/***********************************************************************************/
/*                 tier 3 model - available content duration base                  */
/* db:analytics_workspace schema:content                                           */
/* input tables: fds_nplus.fact_daily_subscription_status_plus                     */
/*               al_t3_vc_content_cluster                                          */
/* ouput tables: al_t3_vc_available_content                                        */
/***********************************************************************************/

/* getting all order history at order user level */
  drop table if exists orders;
create temporary table orders as 
select src_fan_id, order_id, 
       initial_order_date as start_date,
       exprd_entlmnt_date as end_date,
       to_date(to_date(to_char(current_date),'yyyy-mm-dd')) as today_date,   --convert to string so that script can be replaced by another date using python wrapper
       dateadd(mon,-3,today_date) as prev_3_month,   
       dateadd(mon,-12,today_date) as prev_12_month  
  from prod_entdwdb.fds_nplus.fact_daily_subscription_status_plus
 where as_on_date in (select max(as_on_date) from prod_entdwdb.fds_nplus.fact_daily_subscription_status_plus)
   and end_date >= '2019-01-01';  -- for test purpose, remove for production
 
/* 
select count(*) from orders; --14,478,550
select count(*) from orders where end_date >= '2019-01-01'  --5,433,245
select top 100* from orders;   -- src_fan_id
select top 100* from al_t3_vc_content_cluster; */

-- find assets premiering during users' active period (deduped)
  drop table if exists available_asset_list;
create temporary table available_asset_list as
select production_id,
       content_duration,
       content_class,
       any_network_first_stream,
       first_stream as network_first_stream
  from (select production_id, content_duration, content_class,any_network_first_stream,first_stream, 
               row_number()
                    over(partition by production_id,
                                      content_class 
                         order by content_duration desc nulls last) as row_num
          from (select distinct production_id, content_duration, content_class,any_network_first_stream,first_stream
                  from al_t3_vc_content_cluster 
                    -- memory control (include certain content class only)
                 where content_class in ('nxt takeover','tier 1 ppv','tier 2 ppv',--,                               --big events
                                  '205 live','in-ring tournament','nxt','nxt uk','in-ring special'       --in-ring
                                 -- 'historic original','network historic original','network new original'  -- original
                                  )
               )
        )
 where row_num = 1
   and production_id is not null
   and content_duration is not null;

--code to place things in hourly buckets
drop table if exists available_content_user;
create temporary table available_content_user as(
select a.*,
       b.production_id,
       b.content_duration,
       b.content_class,
       b.any_network_first_stream,
       to_date(b.network_first_stream) as network_first_stream
from available_asset_list b
  inner join orders a
     on  b.network_first_stream > a.start_date - 7   -- give 7 day buffer
     and b.network_first_stream < a.end_date + 1);   -- give 1 day buffer


-- dedupe
-- duplicates may happen when a user have multiple orders at the same time
drop table if exists al_t3_vc_available_content;
create table al_t3_vc_available_content as 
select distinct src_fan_id, order_id, start_date,end_date, today_date, prev_3_month, prev_12_month,
                production_id,content_duration,content_class,any_network_first_stream,network_first_stream
  from available_content_user;
  
grant select on al_t3_vc_available_content to public;

select top 100* from al_t3_vc_available_content;

select count(distinct ) from al_t3_vc_available_content;