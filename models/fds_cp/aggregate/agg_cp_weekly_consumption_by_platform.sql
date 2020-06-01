{{
  config(
	materialized='incremental'
    
  )
}}



with __dbt__CTE__youtube_weekly as (

select sum(views) Views, sum(watch_time_minutes) Minutes_watched,
 date_trunc('week',report_date_dt) as monday_date, 'Youtube' as platform
 from {{ref('rpt_yt_wwe_engagement_daily')}} a
 where monday_date >= current_date-20
group by 3,4
), 
 __dbt__CTE__youtube_weekly_aggregate as (

select a.platform, a.monday_date, a.views, b.minutes_watched, a.prev_views, b.prev_mins,
(views*1.0)/nullif(prev_views,0)-1 as weekly_per_change_views, minutes_watched*1.00/nullif(prev_mins*1.00,0)-1  as weekly_per_change_mins
from 
(select platform, monday_date, views, LAG(views)  over (order by monday_date) as prev_views
from __dbt__CTE__youtube_weekly 
group by platform, monday_date, views) as a
left join 
(select platform, monday_date, minutes_watched, LAG(minutes_watched)  over (order by monday_date) as prev_mins
from __dbt__CTE__youtube_weekly 
group by platform, monday_date, minutes_watched) as b
on a.platform=b.platform
and a.monday_date=b.monday_date 
where a.monday_date < date_trunc('week',current_date)
), 

 __dbt__CTE__facebook_weekly as (

select sum(views_3_seconds) Views, sum(video_view_time_minutes) Minutes_watched,
 date_trunc('week',to_date(dim_date_id,'yyyymmdd')) as monday_date, 'Facebook' as platform
 from {{ref('fact_fb_consumption_parent_video')}} a
 where monday_date >= current_date-20
group by 3,4
),  

__dbt__CTE__facebook_weekly_aggregate as (

select a.platform, a.monday_date, a.views, b.minutes_watched, a.prev_views, b.prev_mins,
(views*1.0)/nullif(prev_views,0)-1 as weekly_per_change_views, minutes_watched*1.00/nullif(prev_mins*1.00,0)-1  as weekly_per_change_mins
from
(select platform, monday_date, views, LAG(views)  over (order by monday_date) as prev_views
from __dbt__CTE__facebook_weekly 
group by platform, monday_date, views) as a
left join 
(select platform, monday_date, minutes_watched, LAG(minutes_watched)  over (order by monday_date) as prev_mins
from __dbt__CTE__facebook_weekly  
group by platform, monday_date, minutes_watched) as b
on a.platform=b.platform
and a.monday_date=b.monday_date 
where a.monday_date < date_trunc('week',current_date)
), 

 __dbt__CTE__twitter_weekly as (

select sum(video_views) Views, sum(post_view_time_secs)/60 Minutes_watched,
 date_trunc('week',to_date(dim_date_id,'yyyymmdd')) as monday_date, 'Twitter' as platform
 from {{ref('fact_tw_consumption_post')}}
 where monday_date >= current_date-20
group by 3,4
), 

 __dbt__CTE__twitter_weekly_aggregate as (

select a.platform, a.monday_date, a.views, b.minutes_watched, a.prev_views, b.prev_mins,
(views*1.0)/nullif(prev_views,0)-1 as weekly_per_change_views,minutes_watched*1.00/nullif(prev_mins*1.00,0)-1  as weekly_per_change_mins
from
(select platform, monday_date, views, LAG(views)  over (order by monday_date) as prev_views
from __dbt__CTE__twitter_weekly 
group by platform, monday_date, views) as a
left join 
(select platform, monday_date, minutes_watched, LAG(minutes_watched)  over (order by monday_date) as prev_mins
from __dbt__CTE__twitter_weekly 
group by platform, monday_date, minutes_watched) as b
on a.platform=b.platform
and a.monday_date=b.monday_date 
where a.monday_date < date_trunc('week',current_date)
),  

__dbt__CTE__instagram_weekly as (
select sum(Views) Views, sum(Minutes_watched) Minutes_watched, monday_date, platform 
from
(select sum(video_views) Views, sum(post_view_time_secs)/60 Minutes_watched,
 date_trunc('week',to_date(dim_date_id,'yyyymmdd')) as monday_date, 'Instagram' as platform
 from {{ref('fact_ig_consumption_post')}}
 where monday_date >= current_date-20
group by 3,4
union all
select sum(impressions) Views, sum(story_view_time_secs)/60 Minutes_watched,
 date_trunc('week',to_date(dim_date_id,'yyyymmdd')) as monday_date, 'Instagram' as platform
from {{ref('fact_ig_consumption_story')}} 
 where monday_date >= current_date-20
group by 3,4)
group by 3,4
), 

 __dbt__CTE__instagram_weekly_aggregate as (

select a.platform, a.monday_date, a.views, b.minutes_watched, a.prev_views, b.prev_mins,
(views*1.0)/nullif(prev_views,0)-1 as weekly_per_change_views, minutes_watched*1.00/nullif(prev_mins*1.00,0)-1  as weekly_per_change_mins
from
(select platform, monday_date, views, LAG(views)  over (order by monday_date) as prev_views
from __dbt__CTE__instagram_weekly 
group by platform, monday_date, views) as a
left join 
(select platform, monday_date, minutes_watched, LAG(minutes_watched)  over (order by monday_date) as prev_mins
from __dbt__CTE__instagram_weekly 
group by platform, monday_date, minutes_watched) as b
on a.platform=b.platform
and a.monday_date=b.monday_date 
where a.monday_date < date_trunc('week',current_date)
),  

__dbt__CTE__snapchat_weekly as (

select sum(Views) Views, sum(Minutes_watched) Minutes_watched, monday_date, platform 
from
(select sum(views) Views, sum(views)/6.0 as Minutes_watched,
 date_trunc('week',story_start) as monday_date, 'Snapchat' as platform
 from {{ref('fact_sc_consumption_story')}}
 where monday_date >= current_date-20
group by 3,4
union all 
select sum(topsnap_views) Views, sum(total_time_viewed_secs)/60.0 Minutes_watched,
 date_trunc('week',snap_time_posted) as monday_date, 'Snapchat' as platform
 from fds_sc.fact_scd_consumption_frame
where monday_date >= current_date-20
group by 3,4)
group by 3,4
), 

 __dbt__CTE__snapchat_weekly_aggregate as (

select a.platform, a.monday_date, a.views, b.minutes_watched, a.prev_views, b.prev_mins,
(views*1.0)/nullif(prev_views,0)-1 as weekly_per_change_views, minutes_watched*1.00/nullif(prev_mins*1.00,0)-1  as weekly_per_change_mins
from
(select platform, monday_date, views, LAG(views)  over (order by monday_date) as prev_views
from __dbt__CTE__snapchat_weekly 
group by platform, monday_date, views) as a
left join 
(select platform, monday_date, minutes_watched, LAG(minutes_watched)  over (order by monday_date) as prev_mins
from __dbt__CTE__snapchat_weekly 
group by platform, monday_date, minutes_watched) as b
on a.platform=b.platform
and a.monday_date=b.monday_date 
where a.monday_date < date_trunc('week',current_date)
),

__dbt__CTE__dotcom_weekly as (

select count(*) Views, sum(play_time)/60  Minutes_watched,
 date_trunc('week',start_time) as monday_date, '.COM/App' as platform
 from {{ref('vw_fact_daily_dotcom_viewership')}}
 where monday_date >= current_date-20
group by 3,4
),  

__dbt__CTE__dotcom_weekly_aggregate as (

select a.platform, a.monday_date, a.views, b.minutes_watched, a.prev_views, b.prev_mins,
(views*1.0)/nullif(prev_views,0)-1 as weekly_per_change_views, minutes_watched*1.00/nullif(prev_mins*1.00,0)-1  as weekly_per_change_mins
from
(select platform, monday_date, views, LAG(views)  over (order by monday_date) as prev_views
from __dbt__CTE__dotcom_weekly 
group by platform, monday_date, views) as a
left join
(select platform, monday_date, minutes_watched, LAG(minutes_watched)  over (order by monday_date) as prev_mins
from __dbt__CTE__dotcom_weekly
group by platform, monday_date, minutes_watched) as b
on a.platform=b.platform
and a.monday_date=b.monday_date 
where a.monday_date < date_trunc('week',current_date)
)

select a.*,
100001 as  etl_batch_id,
'etluser_4a' as etl_insert_user_id,
sysdate etl_insert_rec_dttm,
'' etl_update_user_id,
sysdate etl_update_rec_dttm from (
select * from __dbt__CTE__youtube_weekly_aggregate
 where monday_date = (select max(monday_date) from __dbt__CTE__youtube_weekly_aggregate)
union all
select * from __dbt__CTE__facebook_weekly_aggregate
where monday_date = (select max(monday_date) from __dbt__CTE__facebook_weekly_aggregate)
union all
select * from __dbt__CTE__twitter_weekly_aggregate
where monday_date = (select max(monday_date) from __dbt__CTE__twitter_weekly_aggregate)
union all
select * from __dbt__CTE__instagram_weekly_aggregate
where monday_date = (select max(monday_date) from __dbt__CTE__instagram_weekly_aggregate)
union all
select * from __dbt__CTE__snapchat_weekly_aggregate
where monday_date = (select max(monday_date) from __dbt__CTE__snapchat_weekly_aggregate)
union all
select * from __dbt__CTE__dotcom_weekly_aggregate
where monday_date = (select max(monday_date) from __dbt__CTE__dotcom_weekly_aggregate)
) a