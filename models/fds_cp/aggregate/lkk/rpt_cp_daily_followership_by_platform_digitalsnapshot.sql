{{
  config({
		"materialized": 'table'
  })
}}
select * from {{ref('intm_cp_monthly_yt_platform_aggregate')}}
union all 
select * from hive_udl_cp.da_static_yp_digital_deck_historic --upto '2017-07-01' data can be taken from this table sonce yt_monetozation table doesnt have data upto '2017-07-01'
 where month < '2017-07-01' and platform='YouTube'  
union all
select * from {{ref('intm_cp_monthly_app_platform_view_hrs')}}
union all
select * from {{ref('intm_cp_monthly_app_platform_visit_pageview')}}
union all
select * from {{ref('intm_cp_monthly_com_platform_view_hrs')}}
union all
select * from {{ref('intm_cp_monthly_com_platform_visit_pageview')}}
union all
select * from {{ref('intm_cp_monthly_socialmedia_platform_agg')}}
union all
select *
 from hive_udl_cp.da_static_yp_digital_deck_historic --upto '2017-07-01' data can be taken from this table sonce yt_monetozation table doesnt have data upto '2017-07-01'
 where month < '2019-07-01' and platform='Social Media'