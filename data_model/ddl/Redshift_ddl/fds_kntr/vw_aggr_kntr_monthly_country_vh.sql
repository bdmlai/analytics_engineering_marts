/*
******************************************************************************************************************************
Date : 07/24/2020
Version : 1.0
ViewName : vw_aggr_kntr_monthly_country_vh
Schema : fds_kntr
Contributor : Hima Dasan
Description : View calculates actual viewing hour on monthly basis and calculates estimate value for last month for all WWE programs
*************************************************************************************************************************************/
CREATE VIEW
    vw_aggr_kntr_monthly_country_vh
    (
        broadcast_month,
        broadcast_year,
        region,
        src_country,
        broadcast_network_prem_type,
        src_demographic_group,
        src_demographic_age,
        viewing_hours
    ) AS

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
)select substring(broadcast_month_year, 5, 2) as broadcast_month,
       substring(broadcast_month_year, 1, 4) as broadcast_year,
       region, 
	   src_country, 
	   broadcast_network_prem_type, 
	   src_demographic_group, 
	   src_demographic_age, 
case 
	when broadcast_month_year = to_char((add_months(current_date, -1)), 'YYYYMM')  then
	(case
		when (region, src_country, broadcast_network_prem_type, src_demographic_group, src_demographic_age) 
			not in (select distinct region, src_country, broadcast_network_prem_type, 
									src_demographic_group, src_demographic_age
					from __dbt__CTE__intm_kntr_monthly_country_vh
					where broadcast_month_year < to_char((add_months(current_date, -1)), 'YYYYMM'))		
		then
			(case when upper(broadcast_network_prem_type) ='FTA' then
						avg(viewing_hours) over (partition by region, src_country, src_demographic_group, src_demographic_age 
													order by broadcast_month_year desc, broadcast_network_prem_type desc
													rows between 1 following and 3 following)
				else	avg(viewing_hours) over (partition by region, src_country, src_demographic_group, src_demographic_age 
													order by broadcast_month_year desc, broadcast_network_prem_type
													rows between 1 following and 3 following)
			end)
		else
			avg(viewing_hours) over (partition by region, src_country, broadcast_network_prem_type, 
												src_demographic_group, src_demographic_age 
		                              order by broadcast_month_year desc  
									rows between 1 following and 3 following)
	end)
	else (viewing_hours) end as viewing_hours
from  __dbt__CTE__intm_kntr_monthly_country_vh;

COMMENT ON TABLE vw_aggr_kntr_monthly_country_vh
IS
    '## Implementation Detail
*   Date        : 07/24/2020
*   Version     : 1.0
*   ViewName    : vw_aggr_kntr_monthly_country_vh
*   Schema     : fds_kntr
*   Contributor : Hima Dasan
*   Description : View calculates actual viewing hour on monthly basis and calculates estimate value for last month for all WWE programs

## Maintenance Log
* Date : 07/24/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of Phase 4b Project.
* Date : 09/22/2020 ; Developer: Rahul Chandran ; Change: Enhancement has done as requested as per Jira Request: PSTA-1153.'
    ;
COMMENT ON COLUMN vw_aggr_kntr_monthly_country_vh.broadcast_month
IS
    'The broadcast month based on broadcast date';
COMMENT ON COLUMN vw_aggr_kntr_monthly_country_vh.broadcast_year
IS
    'The broadcast year based on broadcast date';
COMMENT ON COLUMN vw_aggr_kntr_monthly_country_vh.region
IS
    'The region of country from where the WWE program is telecasted';
COMMENT ON COLUMN vw_aggr_kntr_monthly_country_vh.src_country
IS
    'The country from where the WWE program is telecasted';
COMMENT ON COLUMN vw_aggr_kntr_monthly_country_vh.broadcast_network_prem_type
IS
    'Indicates whether the channel is Pay / Free To Air';
COMMENT ON COLUMN vw_aggr_kntr_monthly_country_vh.src_demographic_group
IS
    'Broad descriptor for Age Demos (ie P2+, P2-17, P50-99, etc) and Gender Split Demos (ie M2+, F2+)'
    ;
COMMENT ON COLUMN vw_aggr_kntr_monthly_country_vh.src_demographic_age
IS
    'Broad descriptor for Age Demos (ie P2+, P2-17, P50-99, etc) and Gender Split Demos (ie M2+, F2+)'
    ;
COMMENT ON COLUMN vw_aggr_kntr_monthly_country_vh.viewing_hours
IS
    'Average monthly viewing Hours by specified demographic group';