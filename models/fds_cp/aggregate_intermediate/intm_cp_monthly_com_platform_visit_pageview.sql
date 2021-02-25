{{
  config({
		"materialized": 'ephemeral'
  })
}}
select trunc(date_trunc('month',date)) as month,
        geonetwork_country as country,
        case when metric_name='Visits' then '.COM Visits'
             when metric_name='Page Views' then '.COM Page Views'
             when metric_name='Total Unique Visitors' then '.COM Monthly Total Unique Cookiess'
         end  as metric,page,
        sum(coalesce(value,0)) as values, platform         
 from
 (  select date,geonetwork_country, metric_name,
   'NA' as page,
   metric_value as value, '.COM' as platform from {{source('fds_da','dm_digital_kpi_datamart_monthly_topline')}}
      where property = 'WWE.com'
    and geonetwork_us_v_international='Global' and device_type='All' and geonetwork_gm_region_wwe_ref='All'
   and (metric_name='Visits'
         or metric_name='Page Views'
         or metric_name='Total Unique Visitors')
  )
group by month,country,metric,page,platform