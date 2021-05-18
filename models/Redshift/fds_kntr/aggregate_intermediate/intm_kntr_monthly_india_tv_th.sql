{{
  config({
        
		"materialized": 'ephemeral'
  })
}}

SELECT  src_country,
        extract(year FROM dateadd(day,6,week_start_date))           AS rpt_yr,
        extract(month FROM dateadd(day,6,week_start_date))          AS rpt_mn,
        broadcast_network_prem_type,
        series_name,
        live_flag,
        (Sum(CASE WHEN hd_flag = 'No' THEN  duration_mins END)/60)  AS telecast_hours,
        Sum(watched_mins)/60                                        AS viewer_hours
FROM    {{source('fds_kntr','fact_kntr_wwe_telecast_data')}}
WHERE   broadcast_date >= '2019-01-01'      AND
        src_country = 'india'               AND
        src_demographic_group = 'Everyone'  
GROUP BY 1,2,3,4,5,6

