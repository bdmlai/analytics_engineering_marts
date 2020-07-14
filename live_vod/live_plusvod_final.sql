 {{
  config({
	"schema": 'fds_nplus',	
	"materialized": 'incremental',
	"pre-hook":"delete from fds_nplus.rpt_ntwrk_ppv_liveplusvod
				where event_brand in (select distinct event_brand from udl_nplus.raw_da_weekly_live_vod_kickoff_show_dashboard 
				where event_brand in ('PPV','NXT') and data_level = 'Live+VOD' and event_date = current_date)  and data_level = 'Live+VOD'"
		})
}}

live_plus_vod_manual as 
(select * from udl_nplus.raw_da_weekly_live_vod_kickoff_show_dashboard 
where event_brand in ('PPV','NXT') and data_level = 'Live+VOD' 
and event_date = current_date-1),
prior_change_live_vod as
(select a.*, prev_year_views, prev_year_event
  from
  (select platform,
         views as prev_month_views,
         event as prev_month_event,
         event_brand
  from fds_nplus.rpt_ntwrk_ppv_liveplusvod
  where data_level = 'Live+VOD' and event_brand in (select distinct event_brand from live_plus_vod_manual) and event in (select distinct prev_month_event from live_plus_vod_manual)) as a 
  join 
  (
  select platform,
         views as prev_year_views,
         event as prev_year_event,
         event_brand
  from fds_nplus.rpt_ntwrk_ppv_liveplusvod
  where data_level = 'Live+VOD' and event_brand in (select distinct event_brand from live_plus_vod_manual) and event in (select distinct prev_year_event from live_plus_vod_manual)) as b
  on a.platform=b.platform
  and a.event_brand = b.event_brand
  where a.platform <> 'Total'),
live_plusvod_manual_base as 
(select a.*,b.prev_month_views,b.prev_year_views,
case when a.platform = 'YouTube' then round(a.views*0.23)
     else 0 end as us_views
--0 as us_views
from live_plus_vod_manual a
left join
prior_change_live_vod b
on a.platform = b.platform
and a.event_brand = b.event_brand),
liveplus_vod_nwk_views as
(select * 
from (  select *, 'PPV' as event_brand from
        (
         (select 'GLOBAL' as country,   ------- Network Views All
               count(distinct src_fan_id) as views,
               sum(play_time) as minutes 
          from fds_nplus.fact_daily_content_viewership
         where lower(production_id) in (select lower(production_id) from live_plusvod_manual_base where platform = 'Network' and event_brand = 'PPV')   
               and STREAM_START_DTTM between first_stream and _same_day)
        union
        (select 'US' as country,       ------- Network Views US
               count(distinct src_fan_id) as views,
               sum(play_time) as minutes 
          from fds_nplus.fact_daily_content_viewership
         where lower(production_id) in (select lower(production_id) from live_plusvod_manual_base where platform = 'Network' and event_brand = 'PPV')   
           and STREAM_START_DTTM between first_stream and _same_day
           and lower(country_cd) = 'united states')
         )
      )
      union
      (  select *, 'NXT' as event_brand from
        (
         (select 'GLOBAL' as country,   ------- Network Views All
               count(distinct src_fan_id) as views,
               sum(play_time) as minutes 
          from fds_nplus.fact_daily_content_viewership
         where lower(production_id) in (select lower(production_id) from live_plusvod_manual_base where platform = 'Network' and event_brand = 'NXT')   
               and STREAM_START_DTTM between first_stream and _same_day)
        union
        (select 'US' as country,       ------- Network Views US
               count(distinct src_fan_id) as views,
               sum(play_time) as minutes 
          from fds_nplus.fact_daily_content_viewership
         where lower(production_id) in (select lower(production_id) from live_plusvod_manual_base where platform = 'Network' and event_brand = 'NXT')   
           and STREAM_START_DTTM between first_stream and _same_day
           and lower(country_cd) = 'united states')
        )
      )),
live_plusvod_manual_base1 as 
(select report_name,event,event_name,event_brand,series_name,event_date,start_timestamp,end_timestamp,
prev_month_event,prev_year_event,platform,data_level,content_wwe_id,production_id,account,url,asset_id,
case when platform = 'Network' and event_brand = 'PPV' then (select views from liveplus_vod_nwk_views where country = 'GLOBAL' and event_brand = 'PPV') 
     when platform = 'WWE.COM' and event_brand = 'PPV' then (select views from fds_nplus.rpt_ntwrk_ppv_liveplusvod
                               where event_brand = 'PPV' and data_level =  'Live' and event_date = current_date-1 and platform='WWE.COM')
     when platform = 'Twitch' and event_brand = 'PPV' then (select views from fds_nplus.rpt_ntwrk_ppv_liveplusvod
                               where event_brand = 'PPV' and data_level =  'Live' and event_date = current_date-1 and platform='Twitch')
                               
     when platform = 'Network' and event_brand = 'NXT' then (select views from liveplus_vod_nwk_views where country = 'GLOBAL' and event_brand = 'NXT') 
     when platform = 'WWE.COM' and event_brand = 'NXT' then (select views from fds_nplus.rpt_ntwrk_ppv_liveplusvod
                               where event_brand = 'NXT' and data_level =  'Live' and event_date = current_date-1 and platform='WWE.COM')
     when platform = 'Twitch' and event_brand = 'NXT' then (select views from fds_nplus.rpt_ntwrk_ppv_liveplusvod
                               where event_brand = 'NXT' and data_level =  'Live' and event_date = current_date-1 and platform='Twitch')
     else views end as views,
     
case when platform = 'Network' and event_brand = 'PPV' then (select minutes from liveplus_vod_nwk_views where country = 'GLOBAL' and event_brand = 'PPV') 
     when platform = 'Twitch' and event_brand = 'PPV' then (select minutes from fds_nplus.rpt_ntwrk_ppv_liveplusvod
                               where event_brand = 'PPV' and data_level =  'Live' and event_date = current_date-1 and platform='Twitch') 
                                 
     when platform = 'Network' and event_brand = 'NXT' then (select minutes from liveplus_vod_nwk_views where country = 'GLOBAL' and event_brand = 'NXT') 
     when platform = 'Twitch' and event_brand = 'NXT' then (select minutes from fds_nplus.rpt_ntwrk_ppv_liveplusvod
                               where event_brand = 'NXT' and data_level =  'Live' and event_date = current_date-1 and platform='Twitch')   
     else minutes end as minutes,
     
prev_month_views,prev_year_views,
case when platform = 'Network' and event_brand = 'PPV' then (select views from liveplus_vod_nwk_views where country = 'US' and event_brand = 'PPV')
     when platform = 'WWE.COM' and event_brand = 'PPV' then (select us_views from fds_nplus.rpt_ntwrk_ppv_liveplusvod
                               where event_brand = 'PPV' and data_level =  'Live' and event_date = current_date-1 and platform='WWE.COM')
                               
     when platform = 'Network' and event_brand = 'NXT' then (select views from liveplus_vod_nwk_views where country = 'US' and event_brand = 'NXT')
     when platform = 'WWE.COM' and event_brand = 'NXT' then (select us_views from fds_nplus.rpt_ntwrk_ppv_liveplusvod
                               where event_brand = 'NXT' and data_level =  'Live' and event_date = current_date-1 and platform='WWE.COM')
     else us_views end as us_views
from live_plusvod_manual_base),

live_plusvod_manual_base_total as 
(select * from
(
select report_name,event,event_name,event_brand,series_name,event_date,start_timestamp,end_timestamp,
prev_month_event,prev_year_event,platform,data_level,content_wwe_id,production_id,account::varchar,url,asset_id,
views,minutes,prev_month_views,prev_year_views,us_views
from live_plusvod_manual_base1
union all
select report_name,event,event_name,event_brand,series_name,event_date,start_timestamp,end_timestamp,
prev_month_event,prev_year_event,'Total' as platform,data_level,
'' as content_wwe_id,'' as production_id,'' as account,'' as url,'' as asset_id,
sum(views) as views,
sum(minutes) as minutes,
sum(prev_month_views) as prev_month_views,
sum(prev_year_views) as prev_year_views,
sum(us_views) as us_views
from live_plusvod_manual_base1
group by 
report_name,event,event_name,event_brand,series_name,event_date,start_timestamp,end_timestamp,
prev_month_event,prev_year_event,data_level
)),
live_plusvod_consolidation as 
(
select *  from
(
select report_name,event,event_name,event_brand,series_name,event_date,start_timestamp as start_time,end_timestamp as end_time,
prev_month_event,prev_year_event,platform,data_level,content_wwe_id,production_id,account::varchar,url,asset_id,
views,minutes,prev_month_views,prev_year_views,us_views,
case when nvl(us_views,0) > 0 and nvl(views,0) > 0  then (us_views*1.00)/views else null end as per_us_views 
from live_plusvod_manual_base_total
union all
select report_name,event,event_name,event_brand,series_name,event_date,start_time,end_time,
prev_month_event,prev_year_event,platform,data_level,content_wweid,production_id,account,url,asset_id::varchar,
views,minutes,prev_month_views,prev_year_views,us_views,per_us_views 
from fds_nplus.rpt_ntwrk_ppv_liveplusvod where event_brand in (select distinct event_brand from live_plus_vod_manual) and data_level = 'Live+VOD'
and event_date <> current_date-1)),
live_plusvod_final as
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
          case when a.event_date=(select max(event_date) from live_plusvod_consolidation) then 'Most Recent PPV' else 'Prior PPVs' end as Choose_PPV
 from live_plusvod_consolidation a
 left join
           (select platform, event,event_date,views,
           (row_number() OVER (PARTITION BY platform ORDER BY views desc)) as ppv_yearly_rank 
           from live_plusvod_consolidation where current_date-event_date::date <=380 and event_brand = 'PPV') as b
on a.platform=b.platform
and a.event=b.event
and a.event_date=b.event_date

 left join
           (select platform, event,event_date,views,
           (row_number() OVER (PARTITION BY platform ORDER BY views desc)) as nxt_yearly_rank 
           from live_plusvod_consolidation where current_date-event_date::date <=735 and event_brand = 'NXT') as c
on a.platform=c.platform
and a.event=c.event
and a.event_date=c.event_date)
select report_name,series_name,account,url,0 as asset_id,content_wwe_id as content_wweid,production_id,
event,event_name,event_date,start_time,end_time,platform,views,us_views,minutes,per_us_views,
prev_month_views,prev_month_event,prev_year_views,prev_year_event,monthly_per_change_views,
yearly_per_change_views,duration,overall_rank,yearly_rank,tier,monthly_color,yearly_color,
choose_ppv,event_brand,data_level
from live_plusvod_final