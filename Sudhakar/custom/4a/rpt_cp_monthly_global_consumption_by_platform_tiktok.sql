{{
  config({
	"schema": 'fds_cp',
    "pre-hook": "delete from fds_cp.rpt_cp_monthly_global_consumption_by_platform where platform='TikTok' and month = date_trunc('month',current_date-25)",
    "materialized": "incremental"
  })
}}
with #temp_table as 
(select x.platform,
		x.type, 
		'' as type2, 
		nvl(y.country_nm,'Global') as Country,
		x.month, 
		x.views, 
		x.hours_watched, 
		0 as ytd_views, 
		0 as ytd_hours_watched, 
		0 as prev_year_views, 
		0 as prev_year_hours
 from
		(select distinct 
				'TikTok' as platform, 
				b.month, 'TikTok' as type, 
				case when a.month=date_trunc('month',current_date-25)
				then a.views
				else b.views
				end as views,
				case when a.month=date_trunc('month',current_date-25)
				then a.hours_watched
				else b.hours_watched
				end as hours_watched,
				case when a.month=date_trunc('month',current_date-25)
				then a.Country
				else b.Country
				end as Country
		from
				((select date_trunc('month', (date_trunc('month',source_as_on_date)-1)) as month,  
						 country as Country,
						 sum(video_views) views, 
						 sum(play_duration)/3600 hours_watched
				 from {{source('udl_tkt','tiktok_monthly_country_consumption')}}
				 where 	date_trunc('month',source_as_on_date) = date_trunc('month',current_date) 
						and as_on_date = (select max(as_on_date) from {{source('udl_tkt','tiktok_monthly_country_consumption')}})
				 group by 1,2) a
				 right outer join
				(select date_trunc('month',source_as_on_date) as month, 
						country as Country,
						sum(video_views) views, 
						sum(play_duration)/3600 hours_watched
				 from {{source('udl_tkt','tiktok_weekly_country_consumption')}}
				 where 	date_trunc('month',source_as_on_date) = date_trunc('month',current_date-25) 
						and as_on_date = (select max(as_on_date) from {{source('udl_tkt','tiktok_weekly_country_consumption')}})
				 group by date_trunc('month',source_as_on_date),country) b
				 on 1=1)
		) x
left join
cdm.dim_region_country y
		on lower(x.country)=lower(y.iso_alpha2_ctry_cd)
		and src_sys_cd='iso'
		and ent_map_nm='GM Region'
),

#all_data as
((select platform,
		 type, 
		 type2, 
		 country,
		 month, 
		 views, 
		 hours_watched, 
		 ytd_views, 
		 ytd_hours_watched, 
		 prev_year_views, 
		 prev_year_hours 
 from {{source('fds_cp','rpt_cp_monthly_global_consumption_by_platform')}}
 where month between date_trunc('month',add_months(current_date,-14)) and date_trunc('month',current_date-50) and platform ='TikTok')
union
(select * from #temp_table))

(select a.platform, 
		a.type, 
		'' as type2, 
		a.Country, 
		a.month, 
		a.views, 
		a.hours_watched, 
		b.prev_month_views, 
		b.prev_month_hours, 
		d.ytd_views, 
		d.ytd_hours_watched, 
		c.prev_year_views, 
		c.prev_year_hours,
		100001 		 as  etl_batch_id,
		'bi_dbt_user_prd' as etl_insert_user_id,
		sysdate 	 as etl_insert_rec_dttm,
		'' 			 as etl_update_user_id,
		sysdate 	 as	etl_update_rec_dttm
from 
		(select platform, 
				month, 
				type, 
				Country, 
				views, 
				hours_watched
		from #temp_table ) as a
left join 
		(select platform, 
				month, 
				type, 
				Country, 
				views as prev_month_views, 
				hours_watched as prev_month_hours
		from #all_data ) as b
			on a.platform=b.platform
			and a.month=add_months (b.month,1)
			and a.type=b.type
			and a.Country=b.Country
left join
		(select platform, 
				month, 
				type, 
				Country, 
				ytd_views as prev_year_views, 
				ytd_hours_watched as prev_year_hours
		from #all_data) as c
			on a.platform=c.platform
			and add_months(a.month,-12)=c.month
			and a.type=c.type
			and a.Country=c.Country
left join
		(select platform, 
				type, 
				Country,  
				sum(views) ytd_views, 
				sum(hours_watched) ytd_hours_watched
		from #all_data
		where month between date_trunc('year',current_date-25) and date_trunc('month',current_date-25)
		group by 1,2,3) d
			on a.Country=d.Country
			and a.type=d.type
			and a.platform=d.platform
where a.month = date_trunc('month',current_date-25))
