--Weekly Social Followership by platform

/*
*************************************************************************************************************************************************
   Date        : 11/04/2020
   Version     : 1.0
   TableName   : vw_rpt_cp_weekly_social_followers_by_platform
   Schema	   : fds_cp
   Contributor : Sandeep Battula
   Description : Weekly Social Followers by platform view is built on top of reporting table-rpt_cp_weekly_social_followership to fetch the  followers by platform. The followers for previous week, previous year and previous month are also calculated
*************************************************************************************************************************************************
Date : 11/04/2020 ; Developer: Sandeep Battula ; Change: Initial Version as a part of Weekly Social Followers by platform JIRA-PSTA-1047

*/
{{
  config({
	"schema": 'fds_cp',
    "materialized": "view",
	"post-hook" : 'grant select on {{this}} to public'
  })
}}
select * from (
select a.platform,a.as_on_date, total_accounts,a.followers, 
prev_week_followers,
prev_month_followers,
prev_year_followers
from (select as_on_date, platform, count(distinct account) as total_accounts, sum(followers) as followers from {{ref('rpt_cp_weekly_social_followership')}}
group by 1,2) a
left join 
(select as_on_date, platform, sum(followers) as prev_week_followers from {{ref('rpt_cp_weekly_social_followership')}}
group by 1,2) b
on a.as_on_date-7 = b.as_on_date and a.platform = b.platform 
left join 
(select as_on_date, platform, sum(followers) as prev_month_followers from {{ref('rpt_cp_weekly_social_followership')}}
group by 1,2) c
on a.as_on_date-28 = c.as_on_date 
and a.platform = c.platform 
left join 
(select as_on_date, platform, sum(followers) as prev_year_followers from fds_cp.rpt_cp_weekly_social_followership
group by 1,2) d
on extract( week from a.as_on_date) = extract( week from d.as_on_date)
and date_trunc('year',a.as_on_date)-1 = date_trunc('year',d.as_on_date)
and a.platform = d.platform 
order by platform
)