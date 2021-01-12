{{
  config({
		"materialized": 'ephemeral'
  })
}}

select 
a.date,
a.country,
a.property,
sum(a.unique_visitors) unique_visitors,
avg(a.visits) visits,
avg(a.page_views) page_views,
avg(a.video_views) video_views,
sum(b.hours_watched) hours_watched
from
(select date, geonetwork_country as country, property,
sum(case when metric_name= 'Total Unique Visitors' then metric_value else 0 end) as unique_visitors,
sum(case when metric_name= 'Visits' then metric_value else 0 end) as visits,
sum(case when metric_name= 'Page Views' then metric_value else 0 end) as page_views,
sum(case when metric_name= 'Video Views' then metric_value else 0 end) as video_views
from {{source('fds_da','dm_digital_kpi_datamart_monthly_topline')}}
where (property='WWE.com' ) 
and geonetwork_country <>'All' and geonetwork_region='All' and device_type='All' and geonetwork_us_v_international='Global'
and date<(select date_trunc('month',max(date)) from {{source('fds_da','dm_digital_kpi_datamart_monthly_topline')}})  group by 1,2,3
) a 
 full outer join
(select country, to_date(trunc(start_time), 'yyyy-mm-01') as Watch_month,
'WWE.com' as property,
sum(play_time)/3600 as hours_watched
from {{source('fds_nplus','vw_fact_daily_dotcom_viewership')}}
where trunc(start_time) >= to_date('2018-01-01', 'yyyy-mm-01') and trunc(start_time) < to_date(current_date, 'yyyy-mm-01')
group by 2,1,3
order by 2,1,3) b
on a.date=b.watch_month and
lower(a.property)=lower(b.property) and
lower(a.country) = lower(b.country)
where a.date is not null and b.watch_month is not null
group by 1,2,3
