--Weekly Social Followership by account

/*
*************************************************************************************************************************************************
   Date        : 11/04/2020
   Version     : 1.0
   TableName   : vw_rpt_cp_weekly_social_followers_by_account
   Schema	   : fds_cp
   Contributor : Sandeep Battula
   Description : Weekly Social Followers by account view is built on top of reporting table-rpt_cp_weekly_social_followership to fetch the followers at the granularity of account. The followers for previous week, previous month and previous year are also calculated
*************************************************************************************************************************************************
Date : 11/04/2020 ; Developer: Sandeep Battula ; Change: Initial Version as a part of Weekly Social Followers by account JIRA-PSTA-1047
*/
{{
  config({
	"schema": 'fds_cp',
    "materialized": "view"
  })
}}
select a.platform,a.as_on_date, a.account, a.followers, 
INITCAP(a.designation) as designation, a.brand, INITCAP(a.gender) as gender, 
prev_week_followers,
prev_month_followers,
prev_year_followers
from (select as_on_date, platform, account, designation, brand, gender, sum(followers) as followers from {{ref('rpt_cp_weekly_social_followership')}}
group by 1,2,3,4,5,6) a
left join 
(select as_on_date, platform, account, sum(followers) as prev_week_followers from {{ref('rpt_cp_weekly_social_followership')}}
group by 1,2,3) b
on a.as_on_date-7 = b.as_on_date and a.platform = b.platform 
and a.account = b.account
left join 
(select as_on_date, platform, account, sum(followers) as prev_month_followers from {{ref('rpt_cp_weekly_social_followership')}}
group by 1,2,3) c
on a.as_on_date-28 = c.as_on_date
and a.platform = c.platform 
and a.account = c.account
left join 
(select as_on_date, platform, account, sum(followers) as prev_year_followers from fds_cp.rpt_cp_weekly_social_followership
group by 1,2,3) d
on extract( week from a.as_on_date) = extract( week from d.as_on_date)
and date_trunc('year',a.as_on_date)-1 = date_trunc('year',d.as_on_date)
and a.platform = d.platform 
and a.account = d.account