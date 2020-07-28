with __dbt__CTE__intm_kntr_monthly_country_vh as (


select to_char(broadcast_date :: date, 'YYYYMM') as broadcast_month_year,
case 
   when upper(a.src_country)='UNITED ARAB EMIRATES' then 'APAC except India' 
        else b.region end as region,
       src_country, 
       broadcast_network_prem_type, 
	   src_demographic_group, 
	   src_demographic_age, 
sum(watched_mins/60) as viewing_hours 
from "entdwdb"."fds_kntr"."fact_kntr_wwe_telecast_data" a
left join "entdwdb"."fds_kntr"."kantar_static_country_region_tag"  b on upper(a.src_country) = upper(b.country)
where to_char(broadcast_date :: date, 'YYYYMM') < to_char(current_date, 'YYYYMM')
group by 1,2,3,4,5,6
),  __dbt__CTE__intm_kntr_previous_month_value as (


select to_char((add_months(current_date, -1)), 'YYYYMM') as broadcast_month_year, 
region, 
src_country, 
broadcast_network_prem_type, 
src_demographic_group,
 src_demographic_age 
 from (
select  
region, src_country, broadcast_network_prem_type, src_demographic_group, src_demographic_age ,
max(broadcast_month_year) as max_mon_year 
 from __dbt__CTE__intm_kntr_monthly_country_vh
group by 1,2,3,4,5 
) where max_mon_year <> to_char((add_months(current_date, -1)), 'YYYYMM')
)/*
*************************************************************************************************************************************************
   Date        : 07/26/2020
   Version     : 1.0
   TableName   : vw_aggr_kntr_monthly_country_vh
   Schema	   : fds_kntr
   Contributor : Hima Dasan
   Description : View calculates actual viewing hour on monthly basis and calculates estimate value for last month
*************************************************************************************************************************************************
*/



select substring(broadcast_month_year, 5, 2) as broadcast_month,
       substring(broadcast_month_year, 1, 4) as broadcast_year,
       region, 
	   src_country, 
	   broadcast_network_prem_type, 
	   src_demographic_group, 
	   src_demographic_age, 
case 
	when broadcast_month_year = to_char((add_months(current_date, -1)), 'YYYYMM')  then
		avg(viewing_hours) over (
		                          partition by region, src_country, broadcast_network_prem_type, 
		                                       src_demographic_group, src_demographic_age 
		                              order by broadcast_month_year desc  
								 rows between 1 following and 3 following
								)
	else (viewing_hours) end as viewing_hours
from (
      select broadcast_month_year,
             region, src_country, broadcast_network_prem_type, 
			 src_demographic_group, src_demographic_age,viewing_hours 
      from __dbt__CTE__intm_kntr_monthly_country_vh

      union 
      select broadcast_month_year,
             region, src_country, broadcast_network_prem_type, 
			 src_demographic_group, src_demographic_age,0 as viewing_hours 
	  from  __dbt__CTE__intm_kntr_previous_month_value

    )