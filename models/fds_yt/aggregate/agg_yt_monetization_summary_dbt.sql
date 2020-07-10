/*
************************************************************************************ 
    Date: 1/3/2020
    Version: 1.2
    TABLE NAME: 	agg_yt_monetization_summary_dbt
    SCHEMA:       	fds_yt
    Contributor :       Sudhakar
    Description : 	Business critical engagement, consumption and revenue metrics for each uploaded video.
				  It contains attributes like Channel, Country, Geographical region, Title, Series along 
				  with a host of other dimensions which are relevant with respect to the metrics. The measures
				  in this data source primarily revolve around number of views, minutes watched, comments, 
				  shares, like, dislikes and other revenue and demographic details.
*************************************************************************************
*/
/* Date  /  by  / Details  */
/* 5/15/2020  / hima  / added join on channel_name along with video_id to eliminate duplicates with NULL channel name in yt_amg_content_group table for some video_id's */
/* 5/15/2020  / hima  / added distinct on yt_amg_content_group to eliminate duplicate records */

{{
  config({
    "pre-hook": "delete from dwh_read_write.agg_yt_monetization_summary_dbt where view_date between current_date - 52  and current_date - 1",
	"materialized": "incremental",
	"post-hook":["update dwh_read_write.agg_yt_monetization_summary_dbt
	set yt_ad_revenue = Case when b.views <> 0 then a.views * (b.yt_ad_Revenue/(b.views*1.000000)) else a.yt_ad_revenue end,
	ad_impressions = Case when b.views <> 0 then a.views * (b.ad_impressions/(b.views*1.000000)) else a.ad_impressions end,
	partner_revenue = Case when b.views <> 0 then a.views * (b.partner_revenue/(b.views*1.000000)) else a.partner_revenue end,
	subscribers_gained = Case when b.views <> 0 then a.views * (b.subscribers_gained/(b.views*1.000000)) else a.subscribers_gained end,
subscribers_lost = Case when b.views <> 0 then a.views * (b.subscribers_lost/(b.views*1.000000)) else a.subscribers_lost end
from dwh_read_write.agg_yt_monetization_summary_dbt a
join (select channel_name,debut_type,type,owned_class, duration_group,sum(yt_ad_revenue) yt_ad_revenue,sum(views) views,sum(ad_impressions) ad_impressions,sum(partner_revenue) partner_revenue,sum(subscribers_gained) subscribers_gained,sum(subscribers_lost) subscribers_lost
from dwh_read_write.agg_yt_monetization_summary_dbt
where view_date between (select max(view_date) - 31 from dwh_read_write.agg_yt_monetization_summary_dbt) and (select max(view_date) - 2 from dwh_read_write.agg_yt_monetization_summary_dbt)
group by 1,2,3,4,5) b on a.channel_name = b.channel_name and a.debut_type = b.debut_type and a.type = b.type and a.owned_class = b.owned_class
and a.duration_group = b.duration_group
where a.view_date in (select max(view_date) as Maxdate from dwh_read_write.agg_yt_monetization_summary_dbt)",
"update dwh_read_write.agg_yt_monetization_summary_dbt
set yt_ad_revenue = Case when b.views <> 0 then a.views * (b.yt_ad_Revenue/(b.views*1.000000)) else a.yt_ad_revenue end,
ad_impressions = Case when b.views <> 0 then a.views * (b.ad_impressions/(b.views*1.000000)) else a.ad_impressions end,
partner_revenue = Case when b.views <> 0 then a.views * (b.partner_revenue/(b.views*1.000000)) else a.partner_revenue end,
subscribers_gained = Case when b.views <> 0 then a.views * (b.subscribers_gained/(b.views*1.000000)) else a.subscribers_gained end,
subscribers_lost = Case when b.views <> 0 then a.views * (b.subscribers_lost/(b.views*1.000000)) else a.subscribers_lost end
from dwh_read_write.agg_yt_monetization_summary_dbt a
join (select channel_name,debut_type,type,owned_class, duration_group, sum(yt_ad_revenue) yt_ad_revenue,sum(views) views,sum(ad_impressions) ad_impressions,sum(partner_revenue) partner_revenue,sum(subscribers_gained) subscribers_gained,sum(subscribers_lost) subscribers_lost
from dwh_read_write.agg_yt_monetization_summary_dbt
where view_date between (select max(view_date) - 32 from dwh_read_write.agg_yt_monetization_summary_dbt) and (select max(view_date) - 2 from dwh_read_write.agg_yt_monetization_summary_dbt)
group by 1,2,3,4,5) b on a.channel_name = b.channel_name and a.debut_type = b.debut_type and a.type = b.type and a.owned_class = b.owned_class
and a.duration_group = b.duration_group
where a.view_date in (select max(view_date)-1 as Maxdate from dwh_read_write.agg_yt_monetization_summary_dbt)"]})}}


select country_name,country_code,channel_name, view_date, report_date, region, channel_short,region2, country_name2,
views,hours_watched, yt_ad_revenue,ad_impressions,partner_revenue,watch_time_minutes,
likes,
dislikes,
subscribers_gained, 
subscribers_lost,
revenue_views,
male,
female,
gender_other,
AGE_25_34,
AGE_45_54,
AGE_13_17,
AGE_35_44,
AGE_55_64,
AGE_65_,
AGE_18_24,
debut_type, type, owned_class, duration_group,'mb_yt_exec_weekly_trend' as table_name, b.week_name,
current_timestamp::timestamp without time zone as insert_timestamp
from (select * , report_date_dt as view_date,
case when country_name='United States' then 'United States' 
else 'ROW' end as region,
'mb_yt_exec_weekly_trend' as table_name,
case when channel_name='UpUpDownDown' then 'UUDD'
when channel_name='WWE' then 'WWE'
when channel_name='UGC' then 'UGC'
else 'Other' end as channel_short
from  
(
(select * from {{ ref('monetize_summary_wwe') }}
union all
select * from {{ ref('monetize_summary_ugc') }})
)) a
join 
(select full_date, Case when cal_year_week_num = 53 then 'Wk-1,' || cal_year + 1 else 'Wk-' || cal_year_week_num || ', ' || cal_year end as week_name from 
cdm.dim_date 
where full_date between current_date - 52 and current_date - 1) b on a.view_date = b.full_date