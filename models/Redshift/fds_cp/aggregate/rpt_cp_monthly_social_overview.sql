/*
*************************************************************************************************************************************************
   Date        : 11/13/2020
   Version     : 1.0
   TableName   : rpt_cp_monthly_social_overview
   Schema	   : fds_cp
   Contributor : Sandeep Battula
   Description : Monthly Social reporting table consists of social consumption, engagemenet and followership data for social platforms. It also has corresponding account name, country and region details  
*************************************************************************************************************************************************
*/
{{
  config({
		'schema': 'fds_cp',
		"pre-hook": "delete from fds_cp.rpt_cp_monthly_social_overview where month=trunc(date_add('month',-1,date_trunc('month',current_date)))",
		"materialized": 'incremental','tags': "Content"
  })
}}
with #fb_post_volume_monthly as 
(
select  month, 
		dim_platform_id,
		cp_dim_platform,
		dim_smprovider_account_id,
		'Post Volume' as Metric,
		sum(Value) as Value
from
((
select 	date_trunc('month',date_posted) as month,
		dim_platform_id,
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Post Volume' as Metric,
		count(distinct dim_video_id) as Value
from 	{{source('fds_fbk','vw_rpt_daily_fb_published_video')}}
where 	iscrosspost ='false' 
		and month = date_trunc('month',current_date - 28)
group by 1,2,3,4)
union all
(
select 	date_trunc('month',post_date) as month,
		dim_platform_id, 
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Post Volume' as Metric,
		count(distinct dim_media_id) as Value
from 	{{source('fds_fbk','vw_rpt_daily_fb_published_post')}}
where 	month = date_trunc('month',current_date - 28)
		and dim_content_type_id not in ('10236','10003','10230','10234','10257','10260')
group by 1,2,3,4))
group by 1,2,3,4,5),


#tw_post_volume_monthly as (
select 	date_trunc('month',date_posted) as month,
		dim_platform_id,
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Post Volume' as Metric,
		(count (distinct dim_video_id) + count (distinct dim_media_id)) as Value
from 	{{source('fds_tw','vw_rpt_daily_tw_published_post')}}
where 	month = date_trunc('month',current_date - 28)
group by 1,2,3,4),



#ig_post_volume_monthly as (
select  month, 
		dim_platform_id,
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Post Volume' as Metric,
		sum(Value) as Value
from
(select date_trunc('month',post_date) as month,
		dim_platform_id,
		dim_smprovider_account_id,
		(count (distinct dim_video_id) + count (distinct dim_media_id)) as Value
from 	{{source('fds_igm','vw_rpt_daily_ig_published_post')}}
where 	month = date_trunc('month',current_date - 28)
group by 1,2,3

union all

select 	date_trunc('month',frame_date) as month,
		dim_platform_id,
		dim_smprovider_account_id,
		(count (distinct dim_video_id) + count (distinct dim_media_id)) as Value
from  {{source('fds_igm','vw_rpt_daily_ig_published_frame')}}
where 	month = date_trunc('month',current_date - 28)
group by 1,2,3)
group by 1,2,3,4,5),



#fb_consumption_monthly as (
select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
		dim_platform_id,
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Video Views' as Metric,
		sum(views_3_seconds) as Value 
from {{source('fds_fbk','fact_fb_consumption_parent_video')}}
where month = date_trunc('month',current_date-28)
group by 1,2,3,4),



#tw_consumption_monthly as (
select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
		dim_platform_id,
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Video Views' as Metric,
		sum(video_views) as Value 
from {{source('fds_tw','fact_tw_consumption_post')}}
where month = date_trunc('month',current_date-28)
group by 1,2,3,4),


#ig_consumption_monthly as (
select  month, 
		dim_platform_id,
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Video Views' as Metric,
		sum(Value) as Value
from
(select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
		dim_platform_id,
		dim_smprovider_account_id,
		sum(video_views) as Value 
from {{source('fds_igm','fact_ig_consumption_post')}}
where month = date_trunc('month',current_date-28)
group by 1,2,3

union all

select 	date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
		dim_platform_id,
		dim_smprovider_account_id,
		sum(impressions) as Value
from 	{{source('fds_igm','fact_ig_consumption_frame')}}
where 	month = date_trunc('month',current_date - 28)
group by 1,2,3)
group by 1,2,3,4,5
),


#fb_engagement_monthly as (
select  month, 
		dim_platform_id,
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Engagement' as Metric,
		sum(Value) as Value
from
(
select  date_trunc('month',to_date(b.dim_date_id,'yyyymmdd')) as month,
		b.dim_platform_id,
		b.dim_smprovider_account_id,
		(sum(b.likes)+sum(b.comments)+sum(b.shares)) as Value
from {{source('fds_fbk','vw_rpt_daily_fb_published_video')}} a
left join  fds_fbk.fact_fb_engagement_video b
on a.dim_video_id = b.dim_video_id
where a.iscrosspost = false
and month = date_trunc('month',current_date-28)
and a.dim_platform_id = 1
and b.dim_platform_id = 1
group by 1,2,3

union all

select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
		dim_platform_id,
		dim_smprovider_account_id,
		(sum(likes)+sum(comments)+sum(shares)) as Value 
from {{source('fds_fbk','fact_fb_engagement_post')}}
where month = date_trunc('month',current_date-28)
and dim_content_type_id not in ('10236','10003','10230','10234','10257','10260')
group by 1,2,3
)
group by 1,2,3,4,5),


#tw_engagement_monthly as (
select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
		dim_platform_id,
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Engagement' as Metric,
		(sum(likes)+sum(retweets)+sum(replies)) as Value 
from {{source('fds_tw','fact_tw_engagement_post')}}
where month = date_trunc('month',current_date-28)
group by 1,2,3,4),


#ig_engagement_monthly as (
select  month, 
		dim_platform_id,
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Engagement' as Metric,
		sum(Value) as Value
from
(
select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
		dim_platform_id,
		dim_smprovider_account_id,
		(sum(likes)+sum(comments)) as Value 
from {{source('fds_igm','fact_ig_engagement_post')}}
where month = date_trunc('month',current_date-28)
group by 1,2,3

union all

select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
		dim_platform_id,
		dim_smprovider_account_id,
		sum(replies) as Value 
from {{source('fds_igm','fact_ig_engagement_frame')}}
where month = date_trunc('month',current_date-28)
group by 1,2,3
)
group by 1,2,3,4),



#fb_followers_monthly as (
select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')-1) as month,
		1 as dim_platform_id,
		5 as cp_dim_platform,
		dim_smprovider_account_id,
		'Followers' as Metric,
		sum(facebook_followers) as Value 
from {{source('fds_cp','fact_co_smfollowership_cumulative_summary')}}
where to_date(dim_date_id,'yyyymmdd') = date_trunc('month',current_date)
group by 1,2,3,4),


#tw_followers_monthly as (
select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')-1) as month,
		4 as dim_platform_id,
		5 as cp_dim_platform,
		dim_smprovider_account_id,
		'Followers' as Metric,
		sum(twitter_followers) as Value 
from {{source('fds_cp','fact_co_smfollowership_cumulative_summary')}}
where to_date(dim_date_id,'yyyymmdd') = date_trunc('month',current_date)
group by 1,2,3,4),


#ig_followers_monthly as (
select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')-1) as month,
		2 as dim_platform_id,
		5 as cp_dim_platform,
		dim_smprovider_account_id,
		'Followers' as Metric,
		sum(instagram_followers) as Value 
from {{source('fds_cp','fact_co_smfollowership_cumulative_summary')}}
where to_date(dim_date_id,'yyyymmdd') = date_trunc('month',current_date)
group by 1,2,3,4),

#cp_social_platform_data as (
select * from #fb_post_volume_monthly union all
select * from #tw_post_volume_monthly union all
select * from #ig_post_volume_monthly union all
select * from #fb_consumption_monthly union all
select * from #tw_consumption_monthly union all
select * from #ig_consumption_monthly union all
select * from #fb_engagement_monthly union all
select * from #tw_engagement_monthly union all
select * from #ig_engagement_monthly union all
select * from #fb_followers_monthly union all
select * from #tw_followers_monthly union all
select * from #ig_followers_monthly),

#cp_social_latest_data as 
(select month, 
		metric,
		platform,
		account,
		region,
		country,
		sum(value) as value
from
(
select 	a.month, 
		a.metric,
		a.value,
		b.platform_description as platform,
		nvl(c.account_name,'Other') as account,
		nvl(d.wwe_region, 'Other') as region,
		nvl(d.country_market, 'Other') as country
from #cp_social_platform_data a
inner join {{source('cdm','dim_platform')}} as b
on 	a.dim_platform_id = b.dim_platform_id
left join {{source('cdm','dim_smprovider_account')}} c
on  a.dim_smprovider_account_id = c.dim_smprovider_account_id 
and a.cp_dim_platform = c.dim_platform_id
and active_flag = 'true'
left join 
(select * from {{source('hive_udl_cp','daily_social_account_country_mapping')}}
where as_on_date = (select max(as_on_date) from hive_udl_cp.daily_social_account_country_mapping)) d
on  a.dim_smprovider_account_id = d.dim_smprovider_account_id 
and a.cp_dim_platform = d.dim_platform_id)
group by 1,2,3,4,5,6) 


select 	a.* , 
		b.value as prev_month,
		c.value as prev_year,
		100001 		 as  etl_batch_id,
		'bi_dbt_user_prd' as etl_insert_user_id,
		sysdate 	 as etl_insert_rec_dttm,
		'' 			 as etl_update_user_id,
		sysdate 	 as etl_update_rec_dttm
from 	#cp_social_latest_data a 
left join {{source('fds_cp','rpt_cp_monthly_social_overview')}} b
on a.month = add_months(b.month,1)
and a.metric = b.metric
and a.platform = b.platform
and a.account = b.account
left join fds_cp.rpt_cp_monthly_social_overview c
on a.month = add_months(c.month,12)
and a.metric = c.metric
and a.platform = c.platform
and a.account = c.account