{{
  config(
	materialized='ephemeral'
  )
}}
select a.report_date_dt,a.report_date,'UGC' as channel_name,a.country_name,a.country_code,'' as duration_group,'' as debut_type, '' as owned_class, a.region2, a.country_name2,
sum(a.views) as views,
sum(a.hours_watched) as hours_watched,
sum(a.watch_time_minutes) as watch_time_minutes,
sum(b.ad_impressions) as ad_impressions, 
sum(b.estimated_youtube_ad_revenue) as yt_ad_revenue,
sum(b.estimated_partner_revenue) as partner_revenue,
sum(a.likes) likes,
sum(a.dislikes) dislikes,
sum(a.subscribers_gained) subscribers_gained, 
sum(a.subscribers_lost) subscribers_lost,
sum(b.revenue_views) revenue_views,
sum(c.male) male,
sum(c.female) as female,
sum(c.gender_other) gender_other,
sum(c.AGE_25_34) AGE_25_34,
sum(c.AGE_45_54) AGE_45_54,
sum(c.AGE_13_17) AGE_13_17,
sum(c.AGE_35_44) AGE_35_44,
sum(c.AGE_55_64) AGE_55_64,
sum(c.AGE_65_) AGE_65_,
sum(c.AGE_18_24) AGE_18_24,
'UGC' as type from
{{ ref('ugc_engage_measures') }} a
left join 
{{ ref('ugc_revenue_views_uploader_thirdparty') }} b
on 
a.report_date=b.report_date
and a.country_code=b.country_code
left join {{ ref('ugc_demograph_by_gender_age_group') }} c
on a.report_date=c.report_date
and a.country_name=c.country_name
and a.country_code = c.country_code
group by a.report_date,a.report_date_dt, a.region2, a.country_name2, a.country_name,a.country_code