CREATE VIEW VW_AGGR_KNTR_MONTHLY_COUNTRY_VH
AS
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
					from (
select to_char(broadcast_date :: date, 'YYYYMM') as broadcast_month_year,
case 
   when upper(a.src_country)='UNITED ARAB EMIRATES' then 'APAC except India' 
        else b.region end as region,
       src_country, 
       broadcast_network_prem_type, 
	   src_demographic_group, 
	   src_demographic_age, 
sum(watched_mins/60) as viewing_hours 
from fact_kntr_wwe_telecast_data a
left join kantar_static_country_region_tag  b on upper(a.src_country) = upper(b.country)
where to_char(broadcast_date :: date, 'YYYYMM') < to_char(current_date, 'YYYYMM')
group by 1,2,3,4,5,6
)
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
from 
(
select to_char(broadcast_date :: date, 'YYYYMM') as broadcast_month_year,
case 
   when upper(a.src_country)='UNITED ARAB EMIRATES' then 'APAC except India' 
        else b.region end as region,
       src_country, 
       broadcast_network_prem_type, 
	   src_demographic_group, 
	   src_demographic_age, 
sum(watched_mins/60) as viewing_hours 
from fact_kntr_wwe_telecast_data a
left join kantar_static_country_region_tag  b on upper(a.src_country) = upper(b.country)
where to_char(broadcast_date :: date, 'YYYYMM') < to_char(current_date, 'YYYYMM')
group by 1,2,3,4,5,6
);