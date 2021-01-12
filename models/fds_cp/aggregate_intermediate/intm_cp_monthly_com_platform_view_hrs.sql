{{
  config({
		"materialized": 'ephemeral'
  })
}}

select month,country,metric,page,values,platform
from
(select trunc(date_trunc('month',month)) as month,
        country,'.COM Views' as metric,'NA' as page,
        sum(coalesce(Com_views,0)) as values,'.COM' as platform
 from {{ref('intm_cp_monthly_com_platform')}}
 group by month,country,metric    
  )

 union all
 
select month,country,metric,page,values,platform
 from
 (select trunc(date_trunc('month',month)) as month,
        country,'.COM Hours' as metric,'NA' as page,
        sum(coalesce(Com_hrs_watched,0)) as values,'.COM' as platform 
        from {{ref('intm_cp_monthly_com_platform')}}
        group by month,country,metric
  ) 