{{
  config({
		"materialized": 'ephemeral'
  })
}}

select 
substring(date_trunc('month', to_date(report_date,'YYYYMMDD')),1,10) as month, 
country,
'yt_net_subscribers' as metric,
'NA' as page,
netsubs as values,'Social Media' as platform
from
(select a.report_date,a.netsubs, b.country_nm as country from 
(
(
with _data as (
select report_date,
country_code,
sum(subscribers_gained) as subscribers_gained,
sum(subscribers_lost) as subscribers_lost
from {{source('fds_yt','rpt_yt_wwe_engagement_daily')}}
group by 1,2
)
select
  report_date,
  country_code,subscribers_gained, subscribers_lost,
  sum(subscribers_gained) over (partition by country_code order by report_date asc rows between unbounded preceding and current row) as cumulative_gained,
  sum(subscribers_lost) over (partition by country_code order by report_date asc rows between unbounded preceding and current row) as cumulative_lost,
(cumulative_gained - cumulative_lost) as netsubs 
from _data
) as a
left join
(select iso_alpha2_ctry_cd, country_nm from {{source('cdm','dim_region_country')}} where ent_map_nm = 'GM Region'
) as b
on upper(a.country_code) = upper(b.iso_alpha2_ctry_cd)
)
where to_date(report_date,'YYYYMMDD') = last_day(to_date(report_date,'YYYYMMDD'))
) where country is not null