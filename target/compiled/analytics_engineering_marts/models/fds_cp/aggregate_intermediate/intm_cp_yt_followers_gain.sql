

select 
trunc(date_trunc('month',trunc(convert(datetime,convert(varchar(10),report_date))))) as date,
country_name2 as country,
region2 as region,
sum(subscribers_gained) as yt_gain,
sum(subscribers_gained) - sum(subscribers_lost) as yt_followers
from fds_yt.agg_yt_monetization_summary
group by 1,2,3
order by 1,2,3 desc