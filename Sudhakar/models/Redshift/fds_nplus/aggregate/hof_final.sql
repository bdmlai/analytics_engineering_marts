 {{
  config({
	"schema": 'fds_nplus',	
	"materialized": 'incremental',"persist_docs": {'relation' : true, 'columns' : true},
	"pre-hook":["drop table if exists #hof_live_manual;
select report_name,event,event_name,event_brand,series_name,event_date,start_timestamp,end_timestamp,content_wwe_id, production_id,
platform,account,url,asset_id,data_level,views,minutes
into #hof_live_manual 
from udl_nplus.raw_da_weekly_live_vod_kickoff_show_dashboard 
where event_brand = 'Hall Of Fame' and data_level = 'Live' and event_date = trunc(convert_timezone('AMERICA/NEW_YORK', sysdate))
and as_on_date = trunc(convert_timezone('AMERICA/NEW_YORK', sysdate));",
"drop table if exists #prior_change_hoflive;
  select a.*, prev_year_views, prev_year_event
  into #prior_change_hoflive
  from
  (select platform,
         views as prev_month_views,
         event as prev_month_event
  from fds_nplus.rpt_network_ppv_liveplusvod
  where data_level = 'Live' and event_brand = 'Hall Of Fame' 
  and event_date in (select max(event_date) from fds_nplus.rpt_network_ppv_liveplusvod where data_level = 'Live' and event_brand = 'Hall Of Fame' 
					 and event_date <> trunc(convert_timezone('AMERICA/NEW_YORK', sysdate)))) as a 
  join 
  (
  select platform,
         avg(views) as prev_year_views,
         'Avg. Prior Red Carpet Events' as prev_year_event
  from fds_nplus.rpt_network_ppv_liveplusvod
  where data_level = 'Live' and event_brand = 'Hall Of Fame' and event_date < trunc(convert_timezone('AMERICA/NEW_YORK', sysdate))-400
  group by platform, prev_year_event  ) as b
  on a.platform=b.platform;",
  "drop table if exists #hof_live_manual_base;
select a.*,
case when b.prev_month_event is null then 
        (select distinct prev_month_event from #prior_change_hoflive where prev_month_event is not null) 
         else b.prev_month_event end as prev_month_event,
b.prev_month_views,
case when b.prev_year_event is null then 
        (select distinct prev_year_event from #prior_change_hoflive where prev_year_event is not null) 
         else b.prev_year_event end as prev_year_event,
b.prev_year_views, 
case when a.platform = 'YouTube' then round(a.views*0.23)
     else 0 end as us_views
into #hof_live_manual_base
from #hof_live_manual a
left join
#prior_change_hoflive b
on a.platform = b.platform;",
"drop table if exists #hof_live_nwk_unique_viewers;
   select count(distinct a.customerid) as unique_viewers
        into #hof_live_nwk_unique_viewers
        from 
              (    
                select b.*,
                case when max_time >= (select dateadd(m,6,start_timestamp) from #hof_live_manual where platform = 'Network') --  (START TIME + 6 MINUTES, EST)
                and min_time < (select dateadd(m,-4,end_timestamp) from #hof_live_manual where platform = 'Network')         -- (END TIME - 5 MINUTES, EST)
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
                                    (select cast(trunc(start_timestamp)as timestamp) from #hof_live_manual where platform = 'Network') and (select dateadd(m,2,end_timestamp) from #hof_live_manual where platform = 'Network'))       
                                )
                            group by customerid, payload_data_cid 
                            )
                           ) c
                         ) b where b.time_spent>=6
               ) a where a.ppv_flag='1'; 
",
"drop table if exists #hof_live_dotcom_plays;
select max(sum_max_value_plays) as dotcom_plays,
       min(sum_max_value_plays) as dotcom_us_plays
into #hof_live_dotcom_plays
from (
select filter_id, filter_name,
       sum(max_value) as sum_max_value_plays
  from udl_nplus.raw_conviva_pulse_realtime
 where account_name='WWE .Com'
   and filter_id in ('138579','138580')   
   and min_time_est between  
   (select start_timestamp from #hof_live_manual where platform = 'WWE.COM') and (select dateadd(s,59,end_timestamp) from #hof_live_manual where platform = 'WWE.COM') 
 group by filter_id,filter_name);",
 "drop table if exists #hof_live_manual_base1;
select report_name,event,event_name,event_brand,series_name,event_date,start_timestamp,end_timestamp,
prev_month_event,prev_year_event,platform,data_level,content_wwe_id,production_id,account,url,asset_id,
case when platform = 'Network' then (select unique_viewers from #hof_live_nwk_unique_viewers) 
     when platform = 'WWE.COM' then (select dotcom_plays from #hof_live_dotcom_plays)
     else views end as views,
minutes,prev_month_views,prev_year_views,
case when platform = 'Network' then (select unique_viewers*0.75 from #hof_live_nwk_unique_viewers) 
     when platform = 'WWE.COM' then (select dotcom_us_plays from #hof_live_dotcom_plays)
     else us_views end as us_views
into #hof_live_manual_base1
from #hof_live_manual_base;",
"drop table if exists #hof_live_manual_base_total;
select report_name,event,event_name,event_brand,series_name,event_date,start_timestamp,end_timestamp,
prev_month_event,prev_year_event,platform,data_level,content_wwe_id,production_id,account::varchar,url,asset_id,
views,minutes,prev_month_views,
case when platform = 'Total' then (select prev_year_views from #prior_change_hoflive where platform = 'Total')
else prev_year_views end as prev_year_views,
us_views
into #hof_live_manual_base_total from
(
select report_name,event,event_name,event_brand,series_name,event_date,start_timestamp,end_timestamp,
prev_month_event,prev_year_event,platform,data_level,content_wwe_id,production_id,account::varchar,url,asset_id,
views,minutes,prev_month_views,prev_year_views,us_views
from #hof_live_manual_base1
union all
select report_name,event,event_name,event_brand,series_name,event_date,start_timestamp,end_timestamp,
prev_month_event,
prev_year_event,
'Total' as platform,data_level,
'' as content_wwe_id,'' as production_id,'' as account,'' as url,'' as asset_id,
sum(views) as views,
sum(minutes) as minutes,
sum(prev_month_views) as prev_month_views,
sum(prev_year_views) as prev_year_views,
sum(us_views) as us_views
from #hof_live_manual_base1
where platform <> 'Social'
group by 
report_name,event,event_name,event_brand,series_name,event_date,start_timestamp,end_timestamp,
prev_month_event,prev_year_event,data_level
union all
select report_name,event,event_name,event_brand,series_name,event_date,start_timestamp,end_timestamp,
prev_month_event,
prev_year_event,
'Social' as platform,data_level,
'' as content_wwe_id,'' as production_id,'' as account,'' as url,'' as asset_id,
sum(views) as views,
sum(minutes) as minutes,
sum(prev_month_views) as prev_month_views,
sum(prev_year_views) as prev_year_views,
sum(us_views) as us_views
from #hof_live_manual_base1
where platform in ('Facebook','YouTube','Twitter','Twitch')
group by 
report_name,event,event_name,event_brand,series_name,event_date,start_timestamp,end_timestamp,
prev_month_event,prev_year_event,data_level

);",
"drop table if exists #hof_live_consolidation;
select * into #hof_live_consolidation from
(select report_name,event,event_name,event_brand,series_name,event_date,start_timestamp as start_time,end_timestamp as end_time,
prev_month_event,prev_year_event,platform,data_level,content_wwe_id,production_id,account::varchar,url,asset_id,
views,minutes,prev_month_views,prev_year_views,us_views,
case when nvl(us_views,0) > 0 and nvl(views,0) > 0  then (us_views*1.00)/views else null end as per_us_views 
from #hof_live_manual_base_total
union all
select report_name,event,event_name,event_brand,series_name,event_date,start_time,end_time,
prev_month_event,prev_year_event,platform,data_level,content_wweid,production_id,account,url,asset_id::varchar,
views,minutes,prev_month_views,prev_year_views,us_views,per_us_views 
from fds_nplus.rpt_network_ppv_liveplusvod where event_brand in (select event_brand from #hof_live_manual limit 1) and data_level = 'Live'
and event_date <> trunc(convert_timezone('AMERICA/NEW_YORK', sysdate)));",
"drop table if exists #hof_live_final;
select a.*, 
          (a.views*1.00)/a.prev_month_views-1 as monthly_per_change_views,
          (a.views*1.00)/a.prev_year_views-1 as yearly_per_change_views,
          (EXTRACT(EPOCH FROM ((end_time) - (start_time)))/60::numeric)+1 as duration,
          row_number() OVER (PARTITION BY a.platform ORDER BY a.views desc) as overall_rank,
          case when yearly_rank>0 then yearly_rank else null end as yearly_rank,
          case when lower(a.event) like '%wrestlemania%' then 'Tier 1'
               when lower(a.event) like '%royal rumble%' then 'Tier 1'
               when lower(a.event) like '%summerslam%' then 'Tier 1'
               when lower(a.event) like '%survivor series%' then 'Tier 1'
               else 'Tier 2' end as tier,
          case when (a.views*1.00)/a.prev_month_views-1 >= 0 then '1'
          else '0' end as monthly_color,
          case when (a.views*1.00)/a.prev_year_views-1 >= 0 then '1'
          else '0' end as yearly_color,
          case when a.event_date=(select max(event_date) from #hof_live_consolidation) then 'Most Recent PPV' else 'Prior PPVs' end as Choose_PPV
 into #hof_live_final
 from #hof_live_consolidation a
 left join
           (select platform, event,event_date,views,
           (row_number() OVER (PARTITION BY platform ORDER BY views desc)) as yearly_rank 
           from #hof_live_consolidation) as b
on a.platform=b.platform
and a.event=b.event
and a.event_date=b.event_date;",
"delete from fds_nplus.rpt_network_ppv_liveplusvod where event_brand in (select event_brand from #hof_live_manual limit 1) and data_level = 'Live';"]
	})
	}}
select asset_id,production_id,event,event_name,event_date,start_time,end_time,platform,
views,us_views,minutes,per_us_views,prev_month_views,prev_month_event,prev_year_views,prev_year_event,monthly_per_change_views,
yearly_per_change_views,duration,overall_rank,yearly_rank,tier,monthly_color,yearly_color,
choose_ppv,event_brand,report_name,series_name,account,url,content_wwe_id as content_wweid,data_level
'DBT_'+TO_CHAR(convert_timezone('AMERICA/NEW_YORK', sysdate),'YYYY_MM_DD_HH_MI_SS')+'_PPV' etl_batch_id, 'bi_dbt_user_prd' AS etl_insert_user_id,
    convert_timezone('AMERICA/NEW_YORK', sysdate)                                   AS etl_insert_rec_dttm,
    NULL                                                AS etl_update_user_id,
    CAST( NULL AS TIMESTAMP)                            AS etl_update_rec_dttm
from #hof_live_final