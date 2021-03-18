{{
  config({
	"schema": 'fds_cp',
    "materialized": "incremental"
  })
}}
with #yt_global_monthly as 
(select 'YouTube' as platform, 
		type, 
		'' as type2, 
		month, 
		c.country_nm as Country, 
		c.alternate_region_nm as region, 
		sum(views) as views,
		sum(view_time_minutes)/60 as hours_watched
from 
	(select 'Owned' as type, 
			date_trunc('month',to_date(report_date,'yyyymmdd')) as month, 
			country_code, 
			sum(views) as views,
			SUM(watch_time_minutes) AS view_time_minutes
	from fds_yt.rpt_yt_wwe_engagement_daily where month = date_trunc('month',current_date-25)
	group by 1,2,3
	union all 
	select  'UGC' as type, 
			date_trunc('month',to_date(report_date,'yyyymmdd')) as month, 
			country_code, 
			sum(views) as views,
			SUM(watch_time_minutes) AS view_time_minutes
	from fds_yt.rpt_yt_ugc_engagement_daily where month = date_trunc('month',current_date-25)
	group by 1,2,3) a
left join
	cdm.dim_region_country c
			on lower(a.country_code)=lower(c.iso_alpha2_ctry_cd)
			and src_sys_cd='iso'
			and ent_map_nm='GM Region'
where month = date_trunc('month',current_date-25)
group by 1,2,3,4,5,6),

#fb_global_monthly as (
select  'Facebook' as Platform, 
		'Facebook' as type, 
		'' as type2, 
		date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
		'No Geo Detail' as Country,
		'No Geo Detail' as region,
		sum(views_3_seconds) views, 
		sum(video_view_time_minutes)/60 hours_watched
from fds_fbk.fact_fb_consumption_parent_video a
where month = date_trunc('month',current_date-25)
group by 1,2,3,4,5,6),

#tw_global_monthly as (
select  'Twitter' as Platform, 
		'Twitter' as type, 
		'' as type2, 
		date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
		'No Geo Detail' as Country, 
		'No Geo Detail' as region,
		sum(video_views) views, 
		sum(post_view_time_secs)/3600 hours_watched
from fds_tw.fact_tw_consumption_post
where month = date_trunc('month',current_date-25)
group by 1,2,3,4,5,6),

#ig_global_monthly as (
select  'Instagram' as Platform, 
		'Instagram' as type, 
		'' as type2,
		month, 
		'No Geo Detail' as Country, 
		'No Geo Detail' as region,
		sum(views) views, 
		sum(hours_watched) hours_watched
from
	(select date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
			sum(video_views) views, 
			sum(post_view_time_secs)/3600 hours_watched
	 from fds_igm.fact_ig_consumption_post
	 where month = date_trunc('month',current_date-25)
	 group by 1
	 union all
	 select date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
			sum(impressions) Views, 
			sum(story_view_time_secs)/3600 hours_watched
	 from fds_igm.fact_ig_consumption_story 
	 where month = date_trunc('month',current_date-25)
	 group by 1)
group by 1,2,3,4,5,6),

#sc_global_monthly as (
select  'Snapchat' as Platform, 
		'Snapchat' as type, 
		'' as type2, 
		month, 
		'No Geo Detail' as Country, 
		'No Geo Detail' as region,
		sum(views) views, 
		sum(hours_watched) hours_watched
from
	((select date_trunc('month',story_start) as month,
			 sum(views) Views, 
			 sum(views)/360.0 as hours_watched
	 from fds_sc.fact_sc_consumption_story a
	 join
	 (select trunc(story_start) post_date,
			 max(dim_date_id) max_dim_date 
	 from  fds_sc.fact_sc_consumption_story
	 where date_trunc('month',story_start) >= current_date-50 
		   and views>0
	 group by 1) b
	 on trunc(a.story_start) = b.post_date and
	 a.dim_date_id= b.max_dim_date
	 where month = date_trunc('month',current_date-25)
	group by 1)
	union all 
	(select date_trunc('month',snap_time_posted) as month,
			sum(topsnap_views) Views, 
			sum(total_time_viewed_secs)/3600.0 hours_watched
	 from fds_sc.fact_scd_consumption_frame a
	 join
	 (select trunc(snap_time_posted) post_date,
			 max(dim_date_id) max_dim_date 
	 from fds_sc.fact_scd_consumption_frame
	 where date_trunc('month',snap_time_posted) >= current_date-50 
	 and topsnap_views>0
	 group by 1) b
		 on trunc(a.snap_time_posted) = b.post_date and
		 a.dim_date_id= b.max_dim_date
	 where month = date_trunc('month',current_date-25)
	 group by 1))
 group by 1,2,3,4,5,6),
 
#dotcom_global_monthly as
(select '.COM/App' as platform, 
		case stream_type 
		when 'web' then 'WWE.COM' 
		when 'app' then 'WWE App'
		else 'n/a' end as type, 
		'' as type2, 
		date_trunc('month',start_time) as month, 
		nvl(c.country_nm, 'Other') as Country,  
		nvl(c.alternate_region_nm,'Other') as region,
		count(*) views, 
		sum(play_time)/3600 hours_watched
 from fds_nplus.vw_fact_daily_dotcom_viewership a
 left join
 cdm.dim_region_country c
		on lower(a.country)=lower(c.country_nm)
		and src_sys_cd='iso'
		and ent_map_nm='GM Region'
 where month = date_trunc('month',current_date-25)
 group by 1,2,3,4,5,6),
 
 
 #twitch_global_monthly as (
select  'Twitch' as Platform, 
		'Twitch' as type, 
		'' as type2, 
		date_trunc('month',a.date) as month,
		'No Geo Detail' as Country,
		'No Geo Detail' as region,
		sum(live_views) views, 
		sum(minutes_watched)/60 hours_watched
from
		(select date, 
				as_on_date, 
				minutes_watched, 
				live_views  
		from hive_udl_cp.twitch_monthly_stream_sessions 
		where date_trunc('month',date)= date_trunc('month',current_date-25) ) a
join
		(select date, 
				max(as_on_date) max_as_on_date 
		from hive_udl_cp.twitch_monthly_stream_sessions
		where date_trunc('month',date)= date_trunc('month',current_date-25)
		group by 1) b
	on a.date=b.date
	and a.as_on_date = b.max_as_on_date
group by 1,2,3,4,5,6),

#latest_month as 
(select a.*, 
		0 as ytd_views, 
		0 as ytd_hours_watched,
		0 as prev_year_views, 
		0 as prev_year_hours
from 
		(select * from #yt_global_monthly
		union all
		select * from #fb_global_monthly
		union all
		select * from #tw_global_monthly
		union all
		select * from #ig_global_monthly
		union all
		select * from #sc_global_monthly
		union all
		select * from #dotcom_global_monthly
		union all
		select * from #twitch_global_monthly) a
),

#all_data as
(
(select platform,
		type, 
		type2, 
		month, 
		country,
		region, 
		views, 
		hours_watched, 
		ytd_views, 
		ytd_hours_watched, 
		prev_year_views, 
		prev_year_hours 
from fds_cp.rpt_cp_monthly_global_consumption_by_platform 
where month between date_trunc('month',add_months(current_date,-14)) 
	  and date_trunc('month',current_date-50) 
	  and platform in ('YouTube','Facebook','Twitter','Instagram','Snapchat','.COM/App','Twitch'))
union
(select * from #latest_month)
)

(select a.platform, 
		a.type, 
		'' as type2,  
		a.Region, 
		a.Country, 
		a.month, 
		a.views, 
		b.hours_watched, 
		a.prev_month_views, 
		b.prev_month_hours, 
		d.ytd_views, 
		d.ytd_hours_watched, 
		c.prev_year_views, 
		c.prev_year_hours,
		100001 		 as  etl_batch_id,
		'bi_dbt_prod' as etl_insert_user_id,
		sysdate 	 as etl_insert_rec_dttm,
		'' 			 as etl_update_user_id,
		sysdate 	 as etl_update_rec_dttm
from 
	(select platform, 
			month, 
			type, 
			Country, 
			Region, 
			views, 
			LAG(views)  over (partition by platform, type, Country, Region order by month) as prev_month_views
	from #all_data
	group by platform, month, type, Country, Region, views) as a
left join 
	(select platform, 
			month, 
			type, 
			Country, 
			Region, 
			hours_watched, 
			LAG(hours_watched)  over (partition by platform, type, Country, Region order by month) as prev_month_hours
	from #all_data
	group by platform, month, type, Country, Region, hours_watched) as b
		on a.platform=b.platform
		and a.month=b.month
		and a.type=b.type
		and a.Country=b.Country
		and a.Region=b.Region
left join
	(select platform, 
			month, 
			type, 
			Country, 
			Region, 
			ytd_views as prev_year_views, 
			ytd_hours_watched as prev_year_hours
	from #all_data) as c
		on a.platform=c.platform
		and add_months(a.month,-12)=c.month
		and a.type=c.type
		and a.Country=c.Country
		and a.Region=c.Region
left join
	(select platform, 
			type, 
			Country, 
			Region, 
			sum(views) ytd_views, 
			sum(hours_watched) ytd_hours_watched
	from #all_data
	where month between date_trunc('year',current_date-25) 
		  and date_trunc('month',current_date-25)
	group by 1,2,3,4) d
		on a.Country=d.Country
		and a.Region=d.Region
		and a.type=d.type
		and a.platform=d.platform
where a.month = date_trunc('month',current_date-25))