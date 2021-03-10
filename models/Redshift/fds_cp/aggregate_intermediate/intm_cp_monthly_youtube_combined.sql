{{
  config({
		"materialized": 'ephemeral'
  })
}}
 
  select trunc(date_trunc('month',report_date)) as month,
        country,'UGC Views' as metric,'NA' as page ,
        round(sum(coalesce(ugc_views,0))) as values,
        'YouTube' as platform      
 from  {{ref('intm_cp_monthly_youtube')}}
 group by month,country,metric
 
 union all
 select trunc(date_trunc('month',report_date)) as month,
        country,'Owned Views' as metric,'NA' as page,
        round(sum(coalesce(owned_views,0))) as values,
        'YouTube' as platform       
 from   {{ref('intm_cp_monthly_youtube')}}
 group by month,country,metric
 
 union all
 select trunc(date_trunc('month',report_date)) as month,
        country,'UGC Hours' as metric,'NA' as page,
        round(sum(coalesce(ugc_hours_watched,0))) as values,
        'YouTube' as platform 
 from   {{ref('intm_cp_monthly_youtube')}}
 group by month,country,metric
 
 union all
 select trunc(date_trunc('month',report_date)) as month,
        country,'Owned Hours' as metric,'NA'as page,
        round(sum(coalesce(owned_hours_watched,0))) as values,
        'YouTube' as platform       
 from   {{ref('intm_cp_monthly_youtube')}}
 group by month,country,metric
 
 union all
 select trunc(date_trunc('month',report_date)) as month,
        country,'Owned+UGC Views' as metric,'NA'as page,
        abs(round(sum(coalesce(owned_views,0)))+round(sum(coalesce(ugc_views,0)))) as values,
        'YouTube' as platform       
 from   {{ref('intm_cp_monthly_youtube')}}
 group by month,country,metric
 
 union all
 select trunc(date_trunc('month',report_date)) as month,
        country,'Owned+UGC Hours' as metric,'NA'as page,
       abs(round(sum(coalesce(owned_hours_watched,0)))+round(sum(coalesce(ugc_hours_watched,0)))) as values,
        'YouTube' as platform       
 from   {{ref('intm_cp_monthly_youtube')}}
 group by month,country,metric