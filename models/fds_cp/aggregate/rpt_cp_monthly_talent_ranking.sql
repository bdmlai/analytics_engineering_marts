/*
*************************************************************************************************************************************************
   Date        : 02/01/2021
   Version     : 1.0
   TableName   : rpt_cp_monthly_talent_ranking
   Schema	   : fds_cp
   Contributor : Sandeep Battula
   Description : Monthly Talent Ranking reporting table consists of social consumption, engagemenet, followership data and also merchandise sales data for the talents. Social metrics are fetched for platforms- Youtube Facebook, Instagram and Twitter. It also has corresponding brand, designation and gender details
*************************************************************************************************************************************************
*/

{{
  config({
		'schema': 'fds_cp',
		"pre-hook": "delete from fds_cp.rpt_cp_monthly_talent_ranking where month = date_trunc('month', current_date-28)",
		"materialized": 'incremental'
		})
}}

with #vids as
(select k.*,talent from (
select wed.video_id, trunc(wed.time_uploaded) as upload_date, wed.report_date_dt as view_date,
sum(views) as views, sum(wed.watch_time_minutes) as minutes_watched, sum(likes+comments+coalesce(shares,0)) as engagements
from {{source('fds_yt','rpt_yt_wwe_engagement_daily')}} wed where date_trunc('month',report_date_dt)= date_trunc('month',current_date-28)
group by 1,2,3
) k
left join {{source('fds_yt','yt_amg_content_groups')}} acg on (k.video_id = acg.yt_id)),

#vids2 as 
(SELECT video_id,SPLIT_PART(talent,',',N) as name
FROM (select video_id,talent from #vids group by 1,2)
CROSS JOIN 
      (     
            SELECT N::INT 
            FROM 
            (
  SELECT ROW_NUMBER() OVER (ORDER BY TRUE) AS N FROM (select video_id,talent from #vids group by 1,2)
            )
      ) 
WHERE SPLIT_PART(talent,',',N) IS NOT NULL AND SPLIT_PART(talent,',',N) != '' 
group by 1,2),

#yt_consumption_monthly as 
(select trim(name) as talent,
		date_trunc('month', view_date) as month, 
		'YouTube' as platform,
		'Video Views' as Metric,
		sum(tot_views) as Value
from (select a.video_id,a.view_date,a.views as tot_views,a.minutes_watched as tot_min_wat,
a.engagements as tot_eng, b.name from #vids a
left join (select video_id, name from #vids2) b 
on a.video_id=b.video_id)
group by 1,2),

#fb_consumption_monthly as (
select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
		dim_platform_id,
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Video Views' as Metric,
		sum(views_3_seconds) as Value 
from {{source('fds_fbk','fact_fb_consumption_parent_video')}}
where month = date_trunc('month',current_date-28)
group by 1,2,3,4),

#fb_engagement_monthly as (
select  month, 
		dim_platform_id,
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Engagements' as Metric,
		sum(Value) as Value
from
(
select  date_trunc('month',to_date(b.dim_date_id,'yyyymmdd')) as month,
		b.dim_platform_id,
		b.dim_smprovider_account_id,
		(sum(b.likes)+sum(b.comments)+sum(b.shares)) as Value
from {{source('fds_fbk','vw_rpt_daily_fb_published_video')}} a
left join  fds_fbk.fact_fb_engagement_video b
on a.dim_video_id = b.dim_video_id
where a.iscrosspost = false
and month = date_trunc('month',current_date-28)
and a.dim_platform_id = 1
and b.dim_platform_id = 1
group by 1,2,3

union all

select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
		dim_platform_id,
		dim_smprovider_account_id,
		(sum(likes)+sum(comments)+sum(shares)) as Value 
from {{source('fds_fbk','fact_fb_engagement_post')}}
where month = date_trunc('month',current_date-28)
and dim_content_type_id not in ('10236','10003','10230','10234','10257','10260')
group by 1,2,3
)
group by 1,2,3,4,5),

#fb_followers_monthly as (
select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')-1) as month,
		1 as dim_platform_id,
		5 as cp_dim_platform,
		dim_smprovider_account_id,
		'Followers' as Metric,
		sum(facebook_followers) as Value 
from {{source('fds_cp','fact_co_smfollowership_cumulative_summary')}}
where to_date(dim_date_id,'yyyymmdd') = date_trunc('month',current_date)
group by 1,2,3,4),

#tw_engagement_monthly as (
select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')) as month,
		dim_platform_id,
		dim_platform_id as cp_dim_platform,
		dim_smprovider_account_id,
		'Engagements' as Metric,
		(sum(likes)+sum(retweets)+sum(replies)) as Value 
from {{source('fds_tw','fact_tw_engagement_post')}}
where month = date_trunc('month',current_date-28)
group by 1,2,3,4),

#tw_followers_monthly as (
select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')-1) as month,
		4 as dim_platform_id,
		5 as cp_dim_platform,
		dim_smprovider_account_id,
		'Followers' as Metric,
		sum(twitter_followers) as Value 
from {{source('fds_cp','fact_co_smfollowership_cumulative_summary')}}
where to_date(dim_date_id,'yyyymmdd') = date_trunc('month',current_date)
group by 1,2,3,4),


#ig_followers_monthly as (
select  date_trunc('month',to_date(dim_date_id,'yyyymmdd')-1) as month,
		2 as dim_platform_id,
		5 as cp_dim_platform,
		dim_smprovider_account_id,
		'Followers' as Metric,
		sum(instagram_followers) as Value 
from {{source('fds_cp','fact_co_smfollowership_cumulative_summary')}}
where to_date(dim_date_id,'yyyymmdd') = date_trunc('month',current_date)
group by 1,2,3,4),

#cp_talent_platform_data as (
select * from #fb_consumption_monthly union all
select * from #fb_engagement_monthly union all
select * from #fb_followers_monthly union all
select * from #tw_engagement_monthly union all
select * from #tw_followers_monthly union all
select * from #ig_followers_monthly),

#cp_talent_latest_data as 
(select account as talent,
		month, 
		platform,
		metric,
		sum(value) as value
from
(
select 	a.month, 
		a.metric,
		a.value,
		b.platform_description as platform,
		nvl(c.account_name,'Other') as account
from #cp_talent_platform_data a
inner join cdm.dim_platform as b
on 	a.dim_platform_id = b.dim_platform_id
left join {{source('cdm','dim_smprovider_account')}} c
on  a.dim_smprovider_account_id = c.dim_smprovider_account_id 
and a.cp_dim_platform = c.dim_platform_id
and active_flag = 'true')
group by 1,2,3,4),

#merch_sales as
(select * from 
((select distinct dim_business_unit_id as business_unit,date,dim_shop_site_id_new as site_key_new,site_description,src_item_id as item_id,
src_style_description as style_description,src_category_description as category_description,src_major_category_description as major_category_description,src_talent_description as talent_description,
sum(demand_units) as demand_units, sum(demand_sales) as demand_sales,sum(demand_margin) as demand_margin,
sum(shipped_units) as shipped_units, sum(shipped_sales) as shipped_sales,sum(shipped_margin) as shipped_margin,
sum(src_total_units_on_hand) as total_units_on_hand,sum(src_reserved_units) as reserved_units,
sum(src_available_units) as available_units
from 
(select distinct cast('Shop' as varchar(10)) as dim_business_unit_id,cast(cast(a.order_date_id as varchar(10)) as date) as date,a.dim_shop_site_id_new,
case when dim_shop_site_id_new=1 then 'US shop' when dim_shop_site_id_new=2 then 'UK Shop' when dim_shop_site_id_new=3 then 'EU Shop' when dim_shop_site_id_new=4 then 'US Amazon' when dim_shop_site_id_new=5 then 'UK Amazon' when dim_shop_site_id_new=6 then 'EU Amazon'
when dim_shop_site_id_new=7 then 'EBAY' when dim_shop_site_id_new=8 then 'WALMART' when dim_shop_site_id_new=9 then 'AMAZON' when dim_shop_site_id_new=10 then 'TAPOUT' end as site_description,
a.src_item_id,a.src_style_description,a.src_category_description,a.src_major_category_description,a.src_talent_description,
sum(src_units_ordered) as demand_units,sum(src_units_ordered*isnull(src_selling_price,0)) as demand_sales,sum(src_units_ordered*(src_selling_price-src_unit_cost)) as demand_margin,
sum(src_units_shipped) as shipped_units,sum(src_units_shipped*isnull(src_selling_price,0)) as shipped_sales,sum(src_units_shipped*(src_selling_price-src_unit_cost)) as shipped_margin,
sum(src_total_units_on_hand) as src_total_units_on_hand,sum(src_reserved_units) as src_reserved_units,
sum(src_available_units) as src_available_units
from  ( select distinct src_adj_txn_flag,src_back_order_flag,dim_business_unit_id,dim_customer_email_address_id,dim_order_method_id,src_current_retail_price,src_current_retail_price_local,
 order_date_id,src_order_number,src_order_origin_code,src_order_status,src_order_type,src_original_ref_order_number,src_pre_order_flag,src_prepay_code,src_promo_code,src_return_reason_code,
 src_sequence_number,src_ship_carrier,ship_date_id,src_shipped_flag,dim_shop_site_id,dim_source_sys_id,src_special_instructions,src_unit_cost,src_unit_cost_local,src_wwe_order_number,
 src_selling_price,src_units_ordered,src_units_shipped,dim_shop_site_id_new,src_style_description,src_category_description,src_major_category_description,src_talent_description,src_style_id,
 src_item_id,src_total_units_on_hand,src_reserved_units,src_available_units
 from  (select distinct a.*,b.src_style_id,b.src_style_description,b.src_category_description,b.src_major_category_description,b.src_talent_description,
 case when src_promo_code='EBAY' then 7 when src_promo_code='WALMART' then 8 when src_promo_code='AMAZON' then 9 
 when src_wwe_order_number like '900%' then 10 else dim_shop_site_id end as dim_shop_site_id_new
 FROM  (select distinct a.*,c.src_item_id,b.src_total_units_on_hand,b.src_reserved_units,b.src_available_units 
 from  (select * from {{source('fds_cpg','fact_cpg_sales_detail')}} where dim_shop_site_id<=6 and
 (order_date_id>=0 or ship_date_id>=0) and 
 isnull(dim_order_method_id,0) NOT IN (5) and 
 dim_item_id not in (
 select distinct B.dim_item_id from (select A.*,B.src_item_id from {{source('fds_cpg','dim_cpg_kit_item')}} (nolock) A inner join fds_cpg.dim_cpg_item(nolock) B on A.dim_kit_item_id=B.dim_item_id) A, fds_cpg.dim_cpg_item B where A.src_item_id=B.src_item_id )
 and src_order_number not in (
 SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number FROM {{source('fds_cpg','fact_cpg_sales_header')}} where ltrim(rtrim(src_order_origin_code))='GR' or ltrim(rtrim(src_prepay_code))='F') 
 and src_order_number in (
 SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number FROM {{source('fds_cpg','fact_cpg_sales_header')}} (nolock) where ltrim(rtrim(src_original_ref_order_number))='0') 
 ) a 
 left join {{source('fds_cpg','dim_cpg_item')}} c on a.dim_item_id=c.dim_item_id
 left join {{source('fds_cpg','fact_cpg_inventory')}} b on a.dim_item_id=b.dim_item_id and a.order_date_id=b.dim_date_id and a.dim_business_unit_id=b.dim_business_unit_id and a.dim_shop_site_id=b.dim_shop_site_id) a 
 left join (select * from (select *,row_number() over(partition by src_item_id order by effective_start_datetime desc) as rank from {{source('fds_cpg','dim_cpg_item')}} ) where rank=1) b 
 on a.src_item_id=b.src_item_id) 
 ) a  
group by 1,2,3,4,5,6,7,8,9 
union
select distinct 'Shop' as dim_business_unit_id,cast(cast(a.order_date_id as varchar(10)) as date) as date,a.dim_shop_site_id_new,
case when dim_shop_site_id_new=1 then 'US shop' when dim_shop_site_id_new=2 then 'UK Shop' when dim_shop_site_id_new=3 then 'EU Shop' when dim_shop_site_id_new=4 then 'US Amazon' when dim_shop_site_id_new=5 then 'UK Amazon' when dim_shop_site_id_new=6 then 'EU Amazon'
when dim_shop_site_id_new=7 then 'EBAY' when dim_shop_site_id_new=8 then 'WALMART' when dim_shop_site_id_new=9 then 'AMAZON' when dim_shop_site_id_new=10 then 'TAPOUT' end as site_description,
a.src_item_id,a.src_style_description,a.src_category_description,a.src_major_category_description,a.src_talent_description,
sum(src_kit_units_ordered) as demand_units,sum(src_kit_units_ordered*isnull((src_component_percent/100)*src_kit_selling_price,0)) as demand_sales,sum(src_kit_units_ordered*(((src_component_percent/100)*src_kit_selling_price)-a.src_unit_cost)) as demand_margin,
sum(src_kit_units_shipped) as shipped_units,sum(src_kit_units_shipped*isnull((src_component_percent/100)*src_kit_selling_price,0)) as shipped_sales,sum(src_kit_units_shipped*(((src_component_percent/100)*src_kit_selling_price)-a.src_unit_cost)) as shipped_margin,
sum(src_total_units_on_hand) as src_total_units_on_hand,sum(src_reserved_units) as src_reserved_units,
sum(src_available_units) as src_available_units
from 
 (select distinct src_adj_txn_flag,src_back_order_flag,dim_business_unit_id,dim_customer_email_address_id,dim_order_method_id,src_current_retail_price,src_current_retail_price_local,
 order_date_id,src_order_number,src_order_origin_code,src_order_status,src_order_type,src_original_ref_order_number,src_pre_order_flag,src_prepay_code,src_promo_code,src_return_reason_code,
 src_sequence_number,src_ship_carrier,ship_date_id,src_shipped_flag,dim_shop_site_id,dim_source_sys_id,src_special_instructions,src_unit_cost,src_unit_cost_local,src_wwe_order_number,src_component_percent,
 src_kit_selling_price,src_kit_units_ordered,src_kit_units_shipped,dim_shop_site_id_new,src_style_description,src_category_description,src_major_category_description,src_talent_description,src_style_id,
 src_item_id,0 as src_total_units_on_hand,0 as src_reserved_units,0 as src_available_units
 from 
 (select distinct a.*,b.src_style_id,b.src_style_description,b.src_category_description,b.src_major_category_description,b.src_talent_description,
 case when src_promo_code='EBAY' then 7 when src_promo_code='WALMART' then 8 when src_promo_code='AMAZON' then 9 
 when src_wwe_order_number like '900%' then 10 else dim_shop_site_id end as dim_shop_site_id_new
 FROM 
 (select distinct a.*,c.src_item_id from (select * from {{source('fds_cpg','fact_cpg_sales_detail_kit_component')}} ) a left join {{source('fds_cpg','dim_cpg_item')}} c on a.dim_item_id=c.dim_item_id) a 
 left join (select * from (select *,row_number() over(partition by src_item_id order by effective_start_datetime desc) as rank from {{source('fds_cpg','dim_cpg_item')}} ) where rank=1) b 
 on a.src_item_id=b.src_item_id
 )
 ) a 
group by 1,2,3,4,5,6,7,8,9 ) 
group by 1,2,3,4,5,6,7,8,9 )
	 ----
	 UNION
(select distinct cast('Venue' as varchar(10)) as dim_business_unit_id,cast(cast(date_id as varchar(10)) as date) as date,0 as dim_shop_site_id_new,'Venue' as site_description,
src_item_id,src_style_description,src_category_description,
src_major_category_description,src_talent_description, sum(net_units_sold) as demand_units, sum(total_revenue) as demand_sales, 0 as demand_margin,
0 as shipped_units,0 as shipped_sales,0 as shipped_margin,0 as src_total_units_on_hand,0 as src_reserved_units,0 as src_available_units 
from 
 (select distinct a.date_id,a.dim_event_id,a.src_item_id,a.dim_venue_id,a.quantity_shipped,a.quantity_adjustment,a.quantity_returned,a.compelements,a.net_units_sold,a.selling_price,a.total_revenue,a.complement_revenue,
 b.src_style_description,b.src_category_description,b.src_major_category_description,b.src_talent_description,b.src_style_id
 from
 (select distinct a.dim_agg_sales_id,a.date_id,a.dim_item_id,a.dim_event_id,a.dim_venue_id,a.quantity_shipped,a.quantity_adjustment,a.quantity_returned,
 a.compelements,a.net_units_sold,a.selling_price,a.total_revenue,a.complement_revenue,b.src_item_id
 from {{source('fds_cpg','aggr_cpg_daily_venue_sales')}} a left join {{source('fds_cpg','dim_cpg_item')}} b on a.dim_item_id=b.dim_item_id) a
 left join (select * from (select *,row_number() over(partition by src_item_id order by effective_start_datetime desc) as rank from {{source('fds_cpg','dim_cpg_item')}} ) where rank=1) b 
 on a.src_item_id=b.src_item_id ) 
group by 1,2,3,4,5,6,7,8,9 )
---
) 
where date_trunc('month',date)=date_trunc('month',current_date-28)),

#merch_sales_final as 
(select talent_description as talent, trunc(date_trunc('month',date)) as month, 'Merch Sales' as platform, business_unit as metric, 
sum(demand_sales) as value
from #merch_sales
group by 1,2,3,4
union all
select talent_description as talent, trunc(date_trunc('month',date)) as month, 'Merch Sales' as platform, 'Total' as metric, 
sum(demand_sales) as value
from #merch_sales
group by 1,2,3,4
),

#cp_talent_data as
(select * from #yt_consumption_monthly
union all
select * from #cp_talent_latest_data
union all
select * from #merch_sales_final
),

#latest_emm_brand as 
(select a.*, 
case when (current_date - 30) between start_date and coalesce (end_date, current_date) then 'Active'
else 'Inactive' 
end as status from
(select * from (select character_lineage_wweid, designation, start_date, end_date, row_number() over (partition by character_lineage_wweid order by start_date desc) as row_num
from fds_emm.brand)
where row_num=1) a ),

#latest_emm_designation as 
(select * from (select character_lineage_wweid, designation, row_number() over (partition by character_lineage_wweid order by start_date desc) as row_num
from fds_emm.babyface_heel where (current_date - 30) between start_date and coalesce (end_date, current_date))
where row_num=1)


select distinct A.month, A.platform, A.metric, 
A.value,
B.all_conviva_accounts as talent,
coalesce(E.designation, 'Other') as brand,
INITCAP(coalesce(D.designation, 'Other')) as designation,
INITCAP(coalesce(C.gender, 'Unavailable')) as gender,
coalesce(E.status, 'Unavailable') as status,
 100001 as  etl_batch_id,
sysdate etl_insert_rec_dttm,
'' etl_update_user_id,
sysdate etl_update_rec_dttm,
'bi_dbt_user_prd' as etl_insert_user_id
from 	#cp_talent_data A
join hive_udl_cp.da_monthly_conviva_emm_accounts_mapping B
on lower(A.talent) = lower(B.all_conviva_accounts)
left join fds_mdm.character C
on coalesce (B.character_lineage,'NA') = coalesce (C.character_lineage_name,'NA')
and B.character_lineage <> ' '
Left Join #latest_emm_designation D 
on c.character_lineages_wweid=d.character_lineage_wweid
left join #latest_emm_brand E
on c.character_lineages_wweid=e.character_lineage_wweid
where month=date_trunc('month', current_date-28)