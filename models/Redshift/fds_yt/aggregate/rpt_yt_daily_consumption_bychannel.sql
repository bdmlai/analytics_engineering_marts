
{{
  config({
		'schema': 'fds_yt',
		"pre-hook": "delete from fds_yt.rpt_yt_daily_consumption_bychannel where date between current_date - 52 and current_date - 1 ",
		"materialized": 'incremental','tags': "Content","persist_docs": {'relation' : true, 'columns' : true},
		"post-hook" : 'grant select on {{this}} to public'
  })
}}


select distinct a.view_date as date, a.report_date, channel_name, country_code, type as content_type, nvl(d.country_name,'Other') as country_name,
nvl(d.region_name,'Other') as region_name,
sum(views) views, sum(likes) likes,
sum(dislikes) dislikes, sum(watch_time_minutes) watch_time_mins, sum(subscribers_gained) subscribers_gained, 
sum(subscribers_lost) subscribers_lost, 
sum(revenue_views) revenue_views,
sum(ad_impressions) ad_impressions, 
sum(partner_revenue) estimated_partner_revenue, 
sum(yt_ad_revenue) estimated_partner_ad_revenue, 
sum(male) male, 
sum(female) female, 
sum(gender_other) gender_other, 
sum(age_13_17) age_13_17, 
sum(age_18_24) age_18_24, 
sum(age_25_34) as age_25_34, 
sum(age_35_44) age_35_44, 
sum(age_45_54) age_45_54, 
sum(age_55_64) age_55_64, 
sum(age_65_) age_65_and_above,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_YT' AS etl_batch_id, 
		'bi_dbt_user_prd' as etl_insert_user_id,
		sysdate etl_insert_rec_dttm,
		'' etl_update_user_id,
		sysdate etl_update_rec_dttm 
from {{source('fds_yt','agg_yt_monetization_summary')}} a 
left join 
        (
        select b.iso_alpha2_ctry_cd as ctr_code,
        UPPER(LEFT(c.gbl_country_name,1))+LOWER(SUBSTRING(c.gbl_country_name,2,LEN(c.gbl_country_name))) as country_name,
        b.region_nm as region_name
        from {{source('cdm','dim_region_country')}} b
        inner join {{source('cdm','dim_country')}} c
        on b.dim_country_id = c.dim_country_id
        where b.ent_map_nm = 'GM Region'
        group by 1,2,3 ) d
on lower(a.country_code) = lower(d.ctr_code)
where a.view_date between current_date - 52 and current_date - 1  
group by 1,2,3,4,5,6,7