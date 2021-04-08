{{
  config({
	"schema": 'fds_cp',
    "materialized": "view",
	"post-hook":"grant select on {{this}} to public"
  })
}}
select platform, 
		type, 
		type2,  
		Country,
		b.ent_map_nm as region_type,
		nvl(b.region_nm,'Other') as region,	
		month, 
		views, 
		hours_watched, 
		prev_month_views, 
		prev_month_hours, 
		ytd_views, 
		ytd_hours_watched, 
		prev_year_views, 
		prev_year_hours  
		from {{source('fds_cp','rpt_cp_monthly_global_consumption_by_platform')}} a
		left join {{source('cdm','dim_region_country')}} b
		on  lower(a.country)=lower(b.country_nm)
		where a.country <> 'No Geo Detail' and a.country <> 'Global'
union all		
		select platform, 
		type, 
		type2,  
		Country,
		b.ent_map_nm as region_type,
		country as region,	
		month, 
		views, 
		hours_watched, 
		prev_month_views, 
		prev_month_hours, 
		ytd_views, 
		ytd_hours_watched, 
		prev_year_views, 
		prev_year_hours  
		from {{source('fds_cp','rpt_cp_monthly_global_consumption_by_platform')}} a
		cross join (select distinct ent_map_nm from {{source('cdm','dim_region_country')}}) b
		where a.country IN ('No Geo Detail', 'Global')