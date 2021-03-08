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
	(case
		when (region, src_country, broadcast_network_prem_type, src_demographic_group, src_demographic_age) 
			not in (select distinct region, src_country, broadcast_network_prem_type, 
									src_demographic_group, src_demographic_age
					from {{ref('intm_kntr_monthly_country_vh')}}
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
from  {{ref('intm_kntr_monthly_country_vh')}}