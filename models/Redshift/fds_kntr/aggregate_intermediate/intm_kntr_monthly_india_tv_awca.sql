{{
  config({
        
		"materialized": 'ephemeral'
  })
}}

SELECT  src_country,
        week_start_date,
        series_name,
        Dateadd(day,6,week_start_date) AS week_end_date,
        broadcast_network_prem_type,
        Sum(weekly_cumulative_audience) AS cum_aud
FROM    {{source('fds_kntr','vw_aggr_kntr_schedule_wca_data')}}
WHERE   demographic_group_name = 'Everyone' AND
        week_start_date >= '2019-01-01'   AND 
        src_country = 'india'
GROUP BY 1,2,3,4,5