{{
  config({
		"materialized": 'ephemeral'
  })
}}
select 
trunc(date_trunc('month',trunc(convert(datetime,convert(varchar(10),report_date))))) as date,
country_name2 as country,
sum(subscribers_gained) - sum(subscribers_lost) as yt_followers
from fds_yt.agg_yt_monetization_summary
where country_name2 is not null
group by 1,2
order by 1,2 desc