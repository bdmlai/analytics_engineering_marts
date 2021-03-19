


select 
trunc(date_trunc('month',trunc(convert(datetime,convert(varchar(10),report_date))))) as month,
nvl(region2,'Other') as region_nm,
 nvl(regexp_replace(initcap(country_name2),'[^0-9A-z ,\\.()\\-]',''),'Other') as country_nm,
sum(subscribers_gained) - sum(subscribers_lost) as yt_gain,
 sum(yt_gain) over (partition by region_nm,country_nm order by month asc rows between unbounded preceding and current row) as yt_followers
from "entdwdb"."fds_yt"."agg_yt_monetization_summary"
group by 1,2,3
order by 1,2,3 desc