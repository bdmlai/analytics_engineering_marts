/*
*************************************************************************************************************************************************
   Date        : 07/26/2020
   Version     : 1.0
   TableName   : vw_aggr_kntr_monthly_country_vh
   Schema	   : fds_kntr
   Contributor : Hima Dasan
   Description : View calculates actual viewing hour on monthly basis and calculates estimate value for last month
*************************************************************************************************************************************************
*/

{{
  config({
		 'schema': 'fds_kntr',	
	     "materialized": 'view',"tags": 'Phase4B',"persist_docs": {'relation' : true, 'columns' : true}

        })
}}

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
      from {{ref('intm_kntr_monthly_country_vh')}}

      union 
      select broadcast_month_year,
             region, src_country, broadcast_network_prem_type, 
			 src_demographic_group, src_demographic_age,0 as viewing_hours 
	  from  {{ref('intm_kntr_previous_month_value')}}

    )