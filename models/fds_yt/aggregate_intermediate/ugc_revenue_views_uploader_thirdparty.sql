{{
  config(
	materialized='ephemeral'
  )
}}


select r.report_date,r.country_code,
case when r.country_code='US' then 'United States'
     when r.country_code='IN' then 'India'
     when r.country_code='GB' then 'United Kingdom'
else 'ROW' end as country_name, 
sum(r.ad_impressions) ad_impressions, sum(r.estimated_youtube_ad_revenue) estimated_youtube_ad_revenue, 
sum(r.estimated_partner_revenue) estimated_partner_revenue,
sum(case when r.video_id+r.report_date in 
(select distinct video_id+report_date from {{ref('rpt_yt_revenue_daily')}} where estimated_partner_revenue!=0 group by video_id, report_date)
then views else 0 end) revenue_views
from 
(select report_date,country_code,ad_impressions,estimated_youtube_ad_revenue,estimated_partner_revenue,video_id,views,uploader_type
from {{ ref('rpt_yt_revenue_daily') }} where report_date between to_char(current_date - 52, 'YYYYMMDD')   and to_char(current_date - 1, 'YYYYMMDD') 
and uploader_type in ('self' ,'thirdParty')) r 
where r.report_date between to_char(current_date - 52, 'YYYYMMDD') and to_char(current_date - 1, 'YYYYMMDD')
and r.uploader_type='thirdParty' 
group by 1,2,3