{{
  config({
		"materialized": 'ephemeral'
  })
}}
select date as month,'global' as country,
 'twitter_followers' as metric,'NA' as page ,
   twitter_followers as values,'Social Media' as platform
   from
(select trunc(date_trunc('month',date)) as date,
sum(followers) as twitter_followers
from 
(
select trunc(convert(datetime,convert(varchar(10),dim_date_id))) as date,
sum(twitter_followers)as followers from fds_cp.fact_co_smfollowership_cumulative_summary group by 1
) group by 1)