--Weekly Social Followership summary

/*
*************************************************************************************************************************************************
   Date        : 11/04/2020
   Version     : 1.0
   TableName   : vw_rpt_cp_weekly_social_followers_summary
   Schema	   : fds_cp
   Contributor : Sandeep Battula
   Description : Weekly Social Followers summary view is built on top of reporting table-rpt_cp_weekly_social_followership to fetch the summary of followers. The followers for previous week and previous month are also calculated
*************************************************************************************************************************************************
Date : 11/04/2020 ; Developer: Sandeep Battula ; Change: Initial Version as a part of Weekly Social Followers summary JIRA-PSTA-1047
*/


{{
  config({
	"schema": 'fds_cp',
    "materialized": "view"
  })
}}
select * from (
select a.followers,a.total_accounts,a.as_on_date,
b.prev_week_followers,
c.prev_month_followers
from(
(select as_on_date,count(distinct account) as total_accounts,sum(followers) as followers from {{ref('rpt_cp_weekly_social_followership')}}
group by 1) a
left join 
(select as_on_date,count(distinct account) as total_accounts_last_week,sum(followers) as prev_week_followers from {{ref('rpt_cp_weekly_social_followership')}}
group by 1)b
on a.as_on_date-7=b.as_on_date
left join 
(select as_on_date,count(distinct account) as total_accounts_last_month,sum(followers) as prev_month_followers from {{ref('rpt_cp_weekly_social_followership')}}
group by 1
)c
on a.as_on_date-28 = c.as_on_date
))

