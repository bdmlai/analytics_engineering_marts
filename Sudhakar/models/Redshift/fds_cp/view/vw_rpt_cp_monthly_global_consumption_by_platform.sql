{{
  config({
	"schema": 'fds_cp',
    "materialized": "view"
  })
}}

select platform, 
		type, 
		type2,  
		Country,
		case when country='No Geo Detail' then 'No Geo Detail' else nvl(alternate_region_nm,
		case when platform='TikTok' then 'Global' else 'Other' end) end as region	,	
		month, 
		views, 
		hours_watched, 
		prev_month_views, 
		prev_month_hours, 
		ytd_views, 
		ytd_hours_watched, 
		prev_year_views, 
		prev_year_hours  from {{source('fds_cp','rpt_cp_monthly_global_consumption_by_platform')}} a
		left join {{source('cdm','dim_region_country')}} c
		on  lower(a.country)=lower(c.country_nm)
		and src_sys_cd='iso'
		and ent_map_nm='GM Region'



