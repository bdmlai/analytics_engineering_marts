
select trunc(date_trunc('month',date)) as month,
        geonetwork_country as country,
        case when metric_name='Visits' then 'APP Visits'
             when metric_name='Lifetime Downloads' then 'APP Lifetime Downloads'
             when metric_name='Page Views' then 'APP Page Views'
             when metric_name='Total Unique Visitors' then 'APP Monthly Total Unique Cookies'
         end  as metric,page,
        sum(coalesce(value,0)) as values, platform         
 from
 (  select date,geonetwork_country, metric_name,
   'NA' as page,
   metric_value as value, 'APP' as platform from "entdwdb"."fds_da"."dm_digital_kpi_datamart_monthly_topline"
   where property = 'WWE App'  and geonetwork_us_v_international='Global' 
   and device_type='All' and geonetwork_gm_region_wwe_ref ='All'
   and (metric_name='Visits' or lower(metric_name) = 'lifetime downloads' 
         or metric_name='Page Views'
         or metric_name='Total Unique Visitors')
  )
group by month,country,metric,page,platform