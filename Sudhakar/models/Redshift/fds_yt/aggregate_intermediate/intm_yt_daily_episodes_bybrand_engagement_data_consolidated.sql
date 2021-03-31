{{
  config({
		"materialized": 'ephemeral'
        })
}}

with intm_yt_daily_episodes_bybrand_video_currentdate_minus_three 
as
(
select a.channel_id
	, a.channel_name
	, a.video_id
	, a.title
	, a.country_code
	, a.time_uploaded
	, a.as_on_date_dt
	, a.report_date_dt
	, coalesce((a.views*10000/nullif(b.sum_views_v,0)),0) views
        , coalesce((a.comments*10000/nullif(b.sum_comments_v,0)),0) comments
        , coalesce((a.likes*10000/nullif(b.sum_likes_v,0)),0) likes
        , coalesce((a.dislikes*10000/nullif(b.sum_dislikes_v,0)),0) dislikes
        , coalesce((a.shares*10000/nullif(b.sum_shares_v,0)),0) shares

from 
(
	select channel_id, channel_name
			, video_id, title,country_code
			, time_uploaded, views,comments
			, likes, dislikes,shares
			,as_on_date_dt, report_date_dt
	from (
		select channel_id, channel_name
				, video_id, title
				,country_code,time_uploaded
				,as_on_date_dt, report_date_dt
				, sum(views) as views, sum(comments) as comments
				, sum(likes) as likes, sum(dislikes) as dislikes
				, sum(shares) as shares
		from {{source('fds_yt','rpt_yt_wwe_engagement_daily')}}
		where report_date_dt = current_date-3 
		group by 1,2,3,4,5,6,7,8 
		)
) a
left join 
(
		select video_id as video_id
			 ,sum(views) sum_views_v
			 ,sum(comments) sum_comments_v
			 , sum(likes) sum_likes_v
			 , sum(dislikes) sum_dislikes_v
			 , sum(shares) sum_shares_v
		from {{source('fds_yt','rpt_yt_wwe_engagement_daily')}} 
		where report_date_dt = current_date-3
		group by video_id) b on a.video_id=b.video_id
),

intm_yt_daily_episodes_bybrand_video_currentdate_minus_two 
as
(
select a.channel_id, a.channel_name
, a.video_id, a.title, a.country_code
, a.time_uploaded, b.report_date_dt
,coalesce((a.views*b.views/10000),0) views
,coalesce((a.comments*b.comments/10000),0) comments
,coalesce((a.likes*b.likes/10000),0) likes
,coalesce((a.dislikes*b.dislikes/10000),0) dislikes
,coalesce((a.shares*b.shares/10000),0) shares

 from 
	(
		select * from intm_yt_daily_episodes_bybrand_video_currentdate_minus_three
	 ) a
	left join
	(
		select * from (
		       select video_id, report_date_dt 
			   , views, comments, likes
			   , dislikes, shares
		from {{source('fds_yt','rpt_yt_wwe_engagement_daily')}} 
		where report_date_dt = date(getdate()-2)
		              )
	) b on a.video_id=b.video_id
),
intm_yt_daily_episodes_bybrand_video_currentdate_minus_one
as
(
	select a.channel_id, a.channel_name
		, a.video_id, a.title, a.country_code
		, a.time_uploaded, b.report_date_dt
		,coalesce((a.views*b.views/10000),0) views
		,coalesce((a.comments*b.comments/10000),0) comments
		,coalesce((a.likes*b.likes/10000),0) likes
		,coalesce((a.dislikes*b.dislikes/10000),0) dislikes
		,coalesce((a.shares*b.shares/10000),0) shares
	from 
	(select * from intm_yt_daily_episodes_bybrand_video_currentdate_minus_three ) a
		left join
		(
			select * from (
							select video_id, report_date_dt
								, views, comments
								, likes
								, dislikes
								,shares 
							from {{source('fds_yt','rpt_yt_wwe_engagement_daily')}} 
							where report_date_dt = date(getdate()-1)
							)
		) b on a.video_id=b.video_id
),
intm_yt_daily_episodes_bybrand_video_currentdate
 as
(
	select a.channel_id, a.channel_name
			, a.video_id, a.title, a.country_code
			, a.time_uploaded, b.report_date_dt
			,coalesce((a.views*b.views/10000),0) views
			,coalesce((a.comments*b.comments/10000),0) comments
			,coalesce((a.likes*b.likes/10000),0) likes
			,coalesce((a.dislikes*b.dislikes/10000),0) dislikes
			,coalesce((a.shares*b.shares/10000),0) shares
	from 
	(select * from intm_yt_daily_episodes_bybrand_video_currentdate_minus_three ) a
	left join
	(
		select * from 
				(
					select video_id, report_date_dt 
							, views, comments
							, likes, dislikes
							, shares 
					from {{source('fds_yt','rpt_yt_wwe_engagement_daily')}} 
					where report_date_dt = date(getdate())
				)
	) b on a.video_id=b.video_id
),
intm_yt_daily_episodes_bybrand_video_consolidated_previous_three_days
  as
  (
	 select * from 
		(
			(select * from intm_yt_daily_episodes_bybrand_video_currentdate_minus_two )
				union all 
			(select * from intm_yt_daily_episodes_bybrand_video_currentdate_minus_one )
				union all
			(select * from intm_yt_daily_episodes_bybrand_video_currentdate where report_date_dt is not null)
		    order by report_date_dt desc
		 ) 
	where report_date_dt is not null
  )
	select * from intm_yt_daily_episodes_bybrand_video_consolidated_previous_three_days
	 union all
	select * from {{ref('intm_yt_daily_episodes_bybrand_video_history')}} 
 
 
 