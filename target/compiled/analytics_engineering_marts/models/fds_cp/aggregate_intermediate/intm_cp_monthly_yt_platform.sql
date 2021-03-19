

select to_date(report_date,'YYYYMMDD') as report_date,
country_name2 as country,type, 
sum(case when type='UGC' then views end ) as UGC_views,
sum(case when type='Owned' then views end) as owned_views,
sum(case when type='UGC' then hours_watched end) as UGC_hours_watched,
sum(case when type='Owned' then hours_watched end) as owned_hours_watched
 from "entdwdb"."fds_yt"."agg_yt_monetization_summary"
 where report_date >= '20170101'  and country_name2 is not null
 group by report_date,country_name2,type