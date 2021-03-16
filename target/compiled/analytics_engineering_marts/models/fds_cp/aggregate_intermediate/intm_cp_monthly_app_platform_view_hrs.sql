

select month,country,metric,page,values,platform
from
(select trunc(date_trunc('month',month)) as month,
        country,'APP Views' as metric,'NA' as page,
        sum(coalesce(App_views,0)) as values,'APP' as platform
 from __dbt__CTE__intm_cp_monthly_app_platform   
 group by month,country,metric    
  )

 union all
 
select month,country,metric,page,values,platform
 from
 (select trunc(date_trunc('month',month)) as month,
        country,'APP Hours' as metric,'NA' as page,
        sum(coalesce(app_hrs_watched,0)) as values,'APP' as platform 
        from __dbt__CTE__intm_cp_monthly_app_platform
        group by month,country,metric
  )