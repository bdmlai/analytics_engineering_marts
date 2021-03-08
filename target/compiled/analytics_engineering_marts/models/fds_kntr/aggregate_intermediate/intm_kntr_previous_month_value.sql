

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