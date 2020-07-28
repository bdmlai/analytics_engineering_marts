
with live_manual as
(select * from udl_nplus.raw_da_weekly_live_vod_kickoff_show_dashboard --fds_nplus.live_plus_vod_manual_data 
where event_brand in ('PPV','NXT') and data_level = 'Live' and event_date = current_date),
prior_change_live as
(select a.*, prev_year_views, prev_year_event
  from
  (select platform,
         views as prev_month_views,
         event as prev_month_event,
         event_brand
  from fds_nplus.rpt_network_ppv_liveplusvod
  where data_level = 'Live' and event_brand in (select distinct event_brand from live_manual) and event in (select distinct prev_month_event from live_manual)) as a 
  join 
  (select platform,
         views as prev_year_views,
         event as prev_year_event,
         event_brand
  from fds_nplus.rpt_network_ppv_liveplusvod
  where data_level = 'Live' and event_brand in (select distinct event_brand from live_manual) and event in (select distinct prev_year_event from live_manual)) as b
  on a.platform=b.platform
  and a.event_brand = b.event_brand
  where a.platform <> 'Total'),
live_manual_base as 
(select a.*,b.prev_month_views,b.prev_year_views, 
case when a.platform = 'YouTube' then round(a.views*0.23)
     else 0 end as us_views
from live_manual a
left join
prior_change_live b
on a.platform = b.platform
and a.event_brand = b.event_brand),
live_nwk_unique_viewers as 
(select *
from   (
        select count(distinct a.customerid) as unique_viewers, 'PPV' as event_brand
        from 
              (    
                select b.*,
                case when max_time >= (select dateadd(m,6,start_timestamp) from live_manual where platform = 'Network' and event_brand = 'PPV')  --  (START TIME + 6 MINUTES, EST)
                and min_time < (select dateadd(m,-5,end_timestamp) from live_manual where platform = 'Network' and event_brand = 'PPV')         -- (END TIME - 5 MINUTES, EST)
                then 1 else 0 end as ppv_flag
                from 
                    (
                     select c.*, (EXTRACT(EPOCH FROM (max_time-min_time))/60::numeric) as time_spent  
                     from
                       (
                        select customerid, payload_data_cid,min_time,
                        case when max_time is null then 
                        to_timestamp((current_timestamp AT TIME ZONE 'US/Eastern') AT TIME ZONE 'UTC', 'yyyy-mm-dd hh24:mi:ss')
                        else max_time end as max_time   
                        from    
                           (
                           select customerid, payload_data_cid,
                           min(min_time) as min_time,
                           max(max_time) as max_time     
                           from (     
                                select *,
                                (readable_startedat AT TIME ZONE 'UTC') AT TIME ZONE 'US/Eastern' as min_time,
                                (readable_endedat AT TIME ZONE 'UTC') AT TIME ZONE 'US/Eastern' as max_time
                                from (
                                SELECT distinct 
                                customerid, payload_data_ta,payload_data_cid,
                                timestamp 'epoch' + CAST(payload_data_startedat AS BIGINT)/1000 * interval '1 second' AS readable_startedat,
                                timestamp 'epoch' + CAST(payload_data_endedat AS BIGINT)/1000 * interval '1 second' AS readable_endedat,
                                payload_data_device,payload_data_last_active_at 
                                FROM udl_nplus.stg_dice_stream_flattened 
                                where payload_data_ta in ('LIVE_WATCHING_START','LIVE_WATCHING_END')
                                and (((timestamp 'epoch' + CAST(payload_data_startedat AS BIGINT)/1000 * interval '1 second') AT TIME ZONE 'UTC') AT TIME ZONE 'US/Eastern') 
                                between 
                                    (select start_timestamp from live_manual where platform = 'Network' and event_brand = 'PPV') and (select end_timestamp from live_manual where platform = 'Network' and event_brand = 'PPV'))       
                                )
                            group by customerid, payload_data_cid 
                            )
                           ) c
                         ) b where b.time_spent>=6
               ) a where a.ppv_flag='1'
        
        union
               
        select count(distinct a.customerid) as unique_viewers, 'NXT' as event_brand
        from 
              (    
                select b.*,
                case when max_time >= (select dateadd(m,6,start_timestamp) from live_manual where platform = 'Network' and event_brand = 'NXT')  --  (START TIME + 6 MINUTES, EST)
                and min_time < (select dateadd(m,-5,end_timestamp) from live_manual where platform = 'Network' and event_brand = 'NXT')         -- (END TIME - 5 MINUTES, EST)
                then 1 else 0 end as nxt_flag
                from 
                    (
                     select c.*, (EXTRACT(EPOCH FROM (max_time-min_time))/60::numeric) as time_spent  
                     from
                       (
                        select customerid, payload_data_cid,min_time,
                        case when max_time is null then 
                        to_timestamp((current_timestamp AT TIME ZONE 'US/Eastern') AT TIME ZONE 'UTC', 'yyyy-mm-dd hh24:mi:ss')
                        else max_time end as max_time   
                        from    
                           (
                           select customerid, payload_data_cid,
                           min(min_time) as min_time,
                           max(max_time) as max_time     
                           from (     
                                select *,
                                (readable_startedat AT TIME ZONE 'UTC') AT TIME ZONE 'US/Eastern' as min_time,
                                (readable_endedat AT TIME ZONE 'UTC') AT TIME ZONE 'US/Eastern' as max_time
                                from (
                                SELECT distinct 
                                customerid, payload_data_ta,payload_data_cid,
                                timestamp 'epoch' + CAST(payload_data_startedat AS BIGINT)/1000 * interval '1 second' AS readable_startedat,
                                timestamp 'epoch' + CAST(payload_data_endedat AS BIGINT)/1000 * interval '1 second' AS readable_endedat,
                                payload_data_device,payload_data_last_active_at 
                                FROM udl_nplus.stg_dice_stream_flattened 
                                where payload_data_ta in ('LIVE_WATCHING_START','LIVE_WATCHING_END')
                                and (((timestamp 'epoch' + CAST(payload_data_startedat AS BIGINT)/1000 * interval '1 second') AT TIME ZONE 'UTC') AT TIME ZONE 'US/Eastern') 
                                between 
                                    (select start_timestamp from live_manual where platform = 'Network' and event_brand = 'NXT') and (select end_timestamp from live_manual where platform = 'Network' and event_brand = 'NXT'))       
                                )
                            group by customerid, payload_data_cid 
                            )
                           ) c
                         ) b where b.time_spent>=6
               ) a where a.nxt_flag='1'
       )
),





live_dotcom_plays as 
(select * 
from
(
        select max(sum_max_value_plays) as dotcom_plays,
               min(sum_max_value_plays) as dotcom_us_plays,
               'PPV' as event_brand
        from (
        select filter_id, filter_name,
               sum(max_value) as sum_max_value_plays
          from udl_nplus.raw_conviva_pulse_realtime
         where account_name='WWE .Com'
           and filter_id in ('138579','138580')   
           and min_time_est between 
           (select start_timestamp from live_manual where platform = 'WWE.COM' and event_brand = 'PPV') and (select end_timestamp from live_manual where platform = 'WWE.COM' and event_brand = 'PPV') 
         group by filter_id,filter_name 
         )
union
        select max(sum_max_value_plays) as dotcom_plays,
               min(sum_max_value_plays) as dotcom_us_plays,
               'NXT' as event_brand
        from (
        select filter_id, filter_name,
               sum(max_value) as sum_max_value_plays
          from udl_nplus.raw_conviva_pulse_realtime
         where account_name='WWE .Com'
           and filter_id in ('138579','138580')   
           and min_time_est between 
           (select start_timestamp from live_manual where platform = 'WWE.COM' and event_brand = 'NXT') and (select end_timestamp from live_manual where platform = 'WWE.COM' and event_brand = 'NXT') 
         group by filter_id,filter_name 
         )
)
 ),
 live_manual_base1 as 
(select report_name,event,event_name,event_brand,series_name,event_date,start_timestamp,end_timestamp,
prev_month_event,prev_year_event,platform,data_level,content_wwe_id,production_id,account,url,asset_id,
case when platform = 'Network' and event_brand =  'PPV' then (select unique_viewers from live_nwk_unique_viewers where event_brand =  'PPV') 
     when platform = 'WWE.COM' and event_brand =  'PPV' then (select dotcom_plays from live_dotcom_plays where event_brand =  'PPV')
     when platform = 'Network' and event_brand =  'NXT' then (select unique_viewers from live_nwk_unique_viewers where event_brand =  'NXT') 
     when platform = 'WWE.COM' and event_brand =  'NXT' then (select dotcom_plays from live_dotcom_plays where event_brand =  'NXT')
     else views end as views,
minutes,prev_month_views,prev_year_views,
case when platform = 'Network' and event_brand =  'PPV' then (select unique_viewers*0.75 from live_nwk_unique_viewers where event_brand =  'PPV') 
     when platform = 'WWE.COM' and event_brand =  'PPV' then (select dotcom_us_plays from live_dotcom_plays where event_brand =  'PPV')
     when platform = 'Network' and event_brand =  'NXT' then (select unique_viewers*0.75 from live_nwk_unique_viewers where event_brand =  'NXT') 
     when platform = 'WWE.COM' and event_brand =  'NXT' then (select dotcom_us_plays from live_dotcom_plays where event_brand =  'NXT')
     else us_views end as us_views
from live_manual_base),
live_manual_base_total as 
(select * from
(
select report_name,event,event_name,event_brand,series_name,event_date,start_timestamp,end_timestamp,
prev_month_event,prev_year_event,platform,data_level,content_wwe_id,production_id,account::varchar,url,asset_id,
views,minutes,prev_month_views,prev_year_views,us_views
from live_manual_base1
union all
select report_name,event,event_name,event_brand,series_name,event_date,start_timestamp,end_timestamp,
prev_month_event,prev_year_event,'Total' as platform,data_level,
'' as content_wwe_id,'' as production_id,'' as account,'' as url,'' as asset_id,
sum(views) as views,
sum(minutes) as minutes,
sum(prev_month_views) as prev_month_views,
sum(prev_year_views) as prev_year_views,
sum(us_views) as us_views
from live_manual_base1
group by 
report_name,event,event_name,event_brand,series_name,event_date,start_timestamp,end_timestamp,
prev_month_event,prev_year_event,data_level
)),
live_consolidation as
(select *  from
(
select report_name,event,event_name,event_brand,series_name,event_date,start_timestamp as start_time,end_timestamp as end_time,
prev_month_event,prev_year_event,platform,data_level,content_wwe_id,production_id,account,url,asset_id,
views,minutes,prev_month_views,prev_year_views,us_views,
case when nvl(us_views,0) > 0 and nvl(views,0) > 0  then (us_views*1.00)/views else null end as per_us_views 
from live_manual_base_total
union all
select report_name,event,event_name,event_brand,series_name,event_date,start_time,end_time,
prev_month_event,prev_year_event,platform,data_level,content_wweid,production_id,account,url,asset_id::varchar,
views,minutes,prev_month_views,prev_year_views,us_views,per_us_views 
from fds_nplus.rpt_network_ppv_liveplusvod where event_brand in (select distinct event_brand from live_manual) and data_level = 'Live'
and event_date <> current_date)),

live_final as 
(select a.*, 
          (a.views*1.00)/a.prev_month_views-1 as monthly_per_change_views,
          (a.views*1.00)/a.prev_year_views-1 as yearly_per_change_views,
          (EXTRACT(EPOCH FROM ((end_time) - (start_time)))/60::numeric)+1 as duration,
          row_number() OVER (PARTITION BY a.platform ORDER BY a.views desc) as overall_rank,
          case when a.event_brand = 'PPV' and ppv_yearly_rank>0 then ppv_yearly_rank 
               when a.event_brand = 'NXT' and nxt_yearly_rank>0 then nxt_yearly_rank
               else null end as yearly_rank,
          case when lower(a.event) like '%wrestlemania%' then 'Tier 1'
               when lower(a.event) like '%royal rumble%' and lower(a.event) not like '%greatest%' then 'Tier 1'
               when lower(a.event) like '%summerslam%' then 'Tier 1'
               when lower(a.event) like '%survivor series%' then 'Tier 1'
               else 'Tier 2' end as tier,
          case when (a.views*1.00)/a.prev_month_views-1 >= 0 then '1'
          else '0' end as monthly_color,
          case when (a.views*1.00)/a.prev_year_views-1 >= 0 then '1'
          else '0' end as yearly_color,
          case when a.event_date=(select max(event_date) from live_consolidation) then 'Most Recent PPV' else 'Prior PPVs' end as Choose_PPV
 from live_consolidation a
 left join
           (select platform, event,event_date,views,
           (row_number() OVER (PARTITION BY platform ORDER BY views desc)) as ppv_yearly_rank 
           from live_consolidation where current_date-event_date::date <=380 and event_brand = 'PPV') as b
on a.platform=b.platform
and a.event=b.event
and a.event_date=b.event_date
 left join
           (select platform, event,event_date,views,
           (row_number() OVER (PARTITION BY platform ORDER BY views desc)) as nxt_yearly_rank 
           from live_consolidation where current_date-event_date::date <=735 and event_brand = 'NXT') as c
on a.platform=c.platform
and a.event=c.event
and a.event_date=c.event_date)
select report_name,series_name,account,url,0 as asset_id,content_wwe_id as content_wweid,production_id,
event,event_name,event_date,start_time,end_time,platform,views,us_views,minutes,per_us_views,
prev_month_views,prev_month_event,prev_year_views,prev_year_event,monthly_per_change_views,
yearly_per_change_views,duration,overall_rank,yearly_rank,tier,monthly_color,yearly_color,
choose_ppv,event_brand,data_level from live_final