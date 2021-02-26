{{
  config({
		"materialized": 'ephemeral'
  })
}}

select * from {{ref('fb_consumption_monthly')}} union all
select * from {{ref('fb_engagement_monthly')}} union all
select * from {{ref('fb_followers_monthly')}} union all
select * from {{ref('tw_engagement_monthly')}} union all
select * from {{ref('tw_followers_monthly')}} union all
select * from {{ref('ig_followers_monthly')}}
