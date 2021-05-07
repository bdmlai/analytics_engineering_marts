{{
  config({
		'schema': 'fds_cp',
		"pre-hook": "truncate fds_cp.rpt_cp_daily_int_kpi_ranking_youtube",
		"materialized": 'incremental','tags': "Content","persist_docs": {'relation' : true, 'columns' : true},
		"post-hook" : 'grant select on {{this}} to public'
  })
}}

select a.*, 
b.revenue,
b.gross_revenue, 
c.country_nm, 
c.region,
e.population,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_CP' AS etl_batch_id, 
		'bi_dbt_user_prd' as etl_insert_user_id,
		sysdate etl_insert_rec_dttm,
		'' etl_update_user_id,
		sysdate etl_update_rec_dttm 

from 
--owned content
(select country_code,  to_char(trunc(convert(datetime,convert(varchar(10),report_date))),'YYYYMM') as year_month, 
case when channel_name='WWE' then 'WWE' else 'Other' end as channel_name,'WWE' as content_type, 
case when datediff('day',time_uploaded, report_date_dt)<=30 then 'Recent' else 'Old' end as recency,
sum(views) as views,sum(subscribers_gained-subscribers_lost) as netsubs, sum(watch_time_minutes)/60 as hours_watched 
from {{source('fds_yt','rpt_yt_wwe_engagement_daily')}}
where report_date>20180000 and report_date<=(select floor(max(report_date)/100)*100 from {{source('fds_yt','rpt_yt_wwe_engagement_daily')}})
group by 1,2,3,4,5
union
--ugc content
select country_code,  to_char(trunc(convert(datetime,convert(varchar(10),report_date))),'YYYYMM') as year_month, 
'Other' as channel_name, 'UGC' as content_type,  
case when datediff('day',time_uploaded,report_date_dt)<=30 then 'Recent' else 'Old' end as recency,
sum(views) as views, sum(subscribers_gained-subscribers_lost) as netsubs, sum(watch_time_minutes)/60 as hours_watched 
from {{source('fds_yt','rpt_yt_ugc_engagement_daily')}} where video_id not in 
(select distinct video_id from {{source('fds_yt','rpt_yt_wwe_engagement_daily')}}) and report_date>20180000 and
 report_date<=(select floor(max(report_date)/100)*100
from {{source('fds_yt','rpt_yt_ugc_engagement_daily')}} ) group by 1,2,3,4,5
) a 
left join
--revenue
(select country_code,  to_char(trunc(convert(datetime,convert(varchar(10),report_date))),'YYYYMM') as year_month, 
case when channel_name='WWE' then 'WWE' else 'Other' end as channel_name, 
case when video_id in (select distinct video_id from {{source('fds_yt','rpt_yt_wwe_engagement_daily')}}) then
 'WWE' else 'UGC' end as content_type, 
case when video_id in (select video_id from {{source('fds_yt','rpt_yt_wwe_engagement_daily')}} where 
datediff('day',time_uploaded,report_date_dt)<=30) then 'Recent' when video_id in 
(select video_id from {{source('fds_yt','rpt_yt_ugc_engagement_daily')}} where datediff('day',time_uploaded,report_date_dt)<=30) 
then 'Recent' else 'Old' end as recency,
sum(estimated_partner_revenue) as revenue, sum(gross_revenue) as gross_revenue from {{source('fds_yt','rpt_yt_revenue_daily')}} 
where report_date>20180000
group by 1,2,3,4,5) b
on a.country_code=b.country_code and 
a.year_month=b.year_month and 
a.channel_name=b.channel_name and 
a.content_type=b.content_type and 
a.recency=b.recency
left join 
(select distinct iso_alpha2_ctry_cd, country_nm, region_nm as region from {{source('cdm','dim_region_country')}}
 where ent_map_nm = 'GM Region') c
on lower(a.country_code)=lower(c.iso_alpha2_ctry_cd)
left join
(select case when country_name='USA' then 'United States' else country_name end as Country, sum(population) as Population 
from {{source('cdm','dim_country_population')}} group by 1) e
on upper(e.country)=upper(c.country_nm)