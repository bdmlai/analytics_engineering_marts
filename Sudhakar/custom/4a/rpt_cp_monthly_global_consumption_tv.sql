--Monthly Cross Platform Global Content Consumption

/*
*************************************************************************************************************************************************
   Date        : 07/31/2020
   Version     : 1.0
   TableName   : rpt_cp_monthly_global_consumption_by_platform
   Schema	   : fds_cp
   Contributor : Sandeep Battula
   Description : Monthly Cross Platform Global Content Consumption aggregate table consists of consumption metrics Views and Hours watched with country and region details for all cross platforms. This script inserts last month data for platforms- WWE Network, PPTV, Hulu SVOD, International TV and Domestic TV from respective source tables on monthly basis (15th of every month). Inaddition to the latest month, metrics are also calculated and inserted for previous month, year-to-date and previous year-to-date. 
*************************************************************************************************************************************************
Date        : 10/23/2020
   Version     : 2.0
   TableName   : rpt_cp_monthly_global_consumption_by_platform
   Schema	   : fds_cp
   Contributor : Sandeep Battula
   Description : Enhancement is done for Domestic TV platform to get viewing hours for Live+Same day for the dates Live+7 data is not available.  Also updated prehook to delete current month's data for platforms - Domestic TV, PPTV, Hulu SVOD and WWE Network inaddition to current International TV
*************************************************************************************************************************************************
*/																																		 
{{
  config({
	"schema": 'fds_cp',
	"materialized": "incremental",
	"pre-hook": ["delete from fds_cp.rpt_cp_monthly_global_consumption_by_platform where platform='TV' and type='International' and month >= date_trunc('month',current_date-60)",
	"delete from fds_cp.rpt_cp_monthly_global_consumption_by_platform where platform IN ('WWE Network','PPTV','Hulu SVOD','TV') and month = date_trunc('month',current_date-30)" ,
	"create table #all_data as select * from 
(with #nw_global_monthly as (
select 	'WWE Network' as platform, 
		'WWE Network' as type, 
		'' as type2,
		mnth_start_dt as month, 
		nvl(c.country_nm, 'NA') as Country, 
		sum(mnthly_view_cnt) views, 
		sum(mnthly_hours_watched) hours_watched
from fds_nplus.aggr_monthly_content_location_viewership a
left join
		cdm.dim_region_country c
			on lower(a.country_cd)=lower(c.country_nm)
			and src_sys_cd='iso'
			and ent_map_nm='GM Region'
where 	month = date_trunc('month',current_date-30) 
		and stream_type_cd='live+vod' 
		and subs_tier=95
group by 1,2,3,4,5),

#pptv_global_monthly as (
select 	'PPTV' as platform, 
		'PPTV' as type, 
		'' as type2,
		date_trunc('month',date) as month, 
		'China' as Country, 
		sum(total_views) views, 
		sum(total_mins)/60 hours_watched
from udl_nplus.raw_pptv_viewership_snp_tbl
where 	month = date_trunc('month',current_date-30)
		and as_on_date= (select max(as_on_date) from udl_nplus.raw_pptv_viewership_snp_tbl) 
group by 1,2,3,4,5),


#hulu_global_monthly as (
select  'Hulu SVOD' as Platform, 
		'Hulu SVOD' as type, 
		'' as type2, 
		to_date(('01' || flight_month || flight_year), 'ddmonyyyy') as month,
		'United States' as Country,
		null as views, 
		sum(tot_viewing_hours) hours_watched
from fds_nl.vw_aggr_nl_monthly_hulu_wwe_vh_data a
where month = date_trunc('month',current_date-30)
group by 1,2,3,4,5),


#tv_intl_global_monthly as (
select 	'TV' as platform, 
		'International' as type, 
		a.broadcast_network_prem_type as type2,
	    month,
		nvl(b.country_nm, a.country_nm) as Country, 
        null as views,
        sum(hours) as hours_watched
from
		(select to_date(('01' || broadcast_month || broadcast_year), 'ddmmyyyy') as month,
				case lower(src_country)
				when 'russia' then 'Russian Federation'
				when 'taiwan' then 'Taiwan, Province of China'
				when 'south korea' then 'Korea, Republic of' 
				when 'vietnam' then 'Viet Nam' 
				else INITCAP(src_country)
				end as country_nm,
				broadcast_network_prem_type,
				sum(viewing_hours) as hours
		from fds_kntr.vw_aggr_kntr_monthly_country_vh
		where src_demographic_group='Everyone'
			  and month between date_trunc('month',current_date-60) and date_trunc('month',current_date-30)
		group by 1,2,3) a
left join
		cdm.dim_region_country b
			on lower(a.country_nm)=lower(b.country_nm)
			and src_sys_cd='iso'
			and ent_map_nm='GM Region'
group by 1,2,3,4,5),


#domestic_tv_global_monthly as (
select 'TV' as Platform, 
		'Domestic' as type, 
		'' as type2, 
		month,
        'United States' as Country,
		null as views,
        sum(hours_watched) hours_watched
from
((
select  date_trunc('month',broadcast_date) as month,
		sum(viewing_hours) hours_watched
from fds_nl.vw_rpt_nl_daily_wwe_program_ratings a 
where 	month = date_trunc('month',current_date-30)
		and src_demographic_group='P2-999'
		and src_playback_period_cd='Live+7'
group by 1)
union all
(
select  date_trunc('month',broadcast_date) as month,
		sum(viewing_hours) hours_watched
from fds_nl.vw_rpt_nl_daily_wwe_program_ratings a 
where 	broadcast_date between 
		(select max(broadcast_date) + 1 
		from fds_nl.vw_rpt_nl_daily_wwe_program_ratings 
		where 	month=date_trunc('month',current_date-30)
				and src_demographic_group='P2-999'
				and src_playback_period_cd='Live+7')
		and date_trunc('month',current_date) - 1
		and src_demographic_group='P2-999'
		and src_playback_period_cd='Live+SD'
group by 1)) a
group by 1,2,3,4,5),


#latest_month as 
(select a.*, 
		0 as ytd_views, 
		0 as ytd_hours_watched,
		0 as prev_year_views, 
		0 as prev_year_hours
from 
		(select * from #nw_global_monthly
		union all
		select * from #pptv_global_monthly
		union all
		select * from #hulu_global_monthly
		union all
		select * from #tv_intl_global_monthly
		union all
		select * from #domestic_tv_global_monthly) a
),
##all_data as
(
(select platform,
		type, 
		type2, 
		month, 
		country,
		views, 
		hours_watched, 
		ytd_views, 
		ytd_hours_watched, 
		prev_year_views, 
		prev_year_hours 
from fds_cp.rpt_cp_monthly_global_consumption_by_platform  
where month between date_trunc('month',add_months(current_date,-16)) 
	  and date_trunc('month',current_date-60)  
	  and platform in ('WWE Network','PPTV','Hulu SVOD','TV'))
union
(select * from #latest_month)
)
select * from all_data);"
	]
  })
}}

(select a.platform, 
		a.type, 
		a.type2,  
		a.Country, 
		a.month, 
		a.views, 
		b.hours_watched, 
		a.prev_month_views, 
		b.prev_month_hours, 
		d.ytd_views, 
		d.ytd_hours_watched, 
		c.prev_year_views, 
		c.prev_year_hours,
		100001 		 as  etl_batch_id,
		sysdate 	 as etl_insert_rec_dttm,
		'' 			 as etl_update_user_id,
		sysdate 	 as etl_update_rec_dttm,
		'bi_dbt_user_prd' as etl_insert_user_id
from 
	(select platform, 
			month, 
			type, 
			type2,
			Country,  
			views, 
			LAG(views)  over (partition by platform, type, type2, Country order by month) as prev_month_views
	from #all_data
	group by platform, month, type, type2, Country, views) as a
left join 
	(select platform, 
			month, 
			type, 
			type2,
			Country, 
			hours_watched, 
			LAG(hours_watched)  over (partition by platform, type, type2, Country order by month) as prev_month_hours
	from #all_data
	group by platform, month, type, type2, Country, hours_watched) as b
		on a.platform=b.platform
		and a.month=b.month
		and a.type=b.type
		and coalesce (a.type2, 'NA') = coalesce (b.type2, 'NA')
		and a.Country=b.Country
left join
	(select platform, 
			month, 
			type, 
			type2,
			Country,  
			ytd_views as prev_year_views, 
			ytd_hours_watched as prev_year_hours
	from #all_data) as c
		on a.platform=c.platform
		and add_months(a.month,-12)=c.month
		and a.type=c.type
		and coalesce (a.type2, 'NA') = coalesce (c.type2, 'NA')
		and a.Country=c.Country
left join
	(select platform, 
			type, 
			type2,
			Country,  
			sum(views) ytd_views, 
			sum(hours_watched) ytd_hours_watched
	from #all_data
	where month between date_trunc('year',current_date-30) 
		  and date_trunc('month',current_date-30)
	group by 1,2,3,4) d
		on a.Country=d.Country
		and a.type=d.type
		and coalesce (a.type2, 'NA') = coalesce (d.type2, 'NA')
		and a.platform=d.platform
where  a.month = date_trunc('month',current_date-30))
	   
union all

(select a.platform, 
		a.type, 
		a.type2,  
		a.Country, 
		a.month, 
		a.views, 
		b.hours_watched, 
		a.prev_month_views, 
		b.prev_month_hours, 
		d.ytd_views, 
		d.ytd_hours_watched, 
		c.prev_year_views, 
		c.prev_year_hours,
		100001 		 as  etl_batch_id,
		sysdate 	 as etl_insert_rec_dttm,
		'' 			 as etl_update_user_id,
		sysdate 	 as etl_update_rec_dttm,
		'bi_dbt_user_prd' as etl_insert_user_id
from 
	(select platform, 
			month, 
			type, 
			type2,
			Country,  
			views, 
			LAG(views)  over (partition by platform, type, type2, Country order by month) as prev_month_views
	from #all_data
	group by platform, month, type, type2, Country,  views) as a
left join 
	(select platform, 
			month, 
			type, 
			type2,
			Country, 
			hours_watched, 
			LAG(hours_watched)  over (partition by platform, type, type2, Country order by month) as prev_month_hours
	from #all_data
	group by platform, month, type, type2, Country,  hours_watched) as b
		on a.platform=b.platform
		and a.month=b.month
		and a.type=b.type
		and coalesce (a.type2, 'NA') = coalesce (b.type2, 'NA')
		and a.Country=b.Country
left join
	(select platform, 
			month, 
			type, 
			type2,
			Country, 
			ytd_views as prev_year_views, 
			ytd_hours_watched as prev_year_hours
	from #all_data) as c
		on a.platform=c.platform
		and add_months(a.month,-12)=c.month
		and a.type=c.type
		and coalesce (a.type2, 'NA') = coalesce (c.type2, 'NA')
		and a.Country=c.Country
left join
	(select platform, 
			type, 
			type2,
			Country, 
			sum(views) ytd_views, 
			sum(hours_watched) ytd_hours_watched
	from #all_data
	where month between date_trunc('year',current_date-60) 
		  and date_trunc('month',current_date-60)
	group by 1,2,3,4) d
		on a.Country=d.Country
		and a.type=d.type
		and coalesce (a.type2, 'NA') = coalesce (d.type2, 'NA')
		and a.platform=d.platform
where (a.platform = 'TV' and a.type='International' and 
	   a.month = date_trunc('month',current_date-60))
)