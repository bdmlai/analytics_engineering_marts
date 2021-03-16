
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