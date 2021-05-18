{{
  config({
        
		"materialized": 'ephemeral'
  })
}}

 SELECT src_country,
        broadcast_network_prem_type,
        series_name,
        week_start_date,
        Dateadd(day,6,week_start_date) AS week_end_date,
        Sum(aud)                       AS aud_sum
FROM    {{source('fds_kntr','fact_kntr_wwe_telecast_data')}} 
WHERE   demographic_group_name = 'Everyone'     AND 
        week_start_date >= '2019-01-01'         AND 
        src_country = 'india'                   AND
        duration_mins > 50                      AND 
        (   
            src_series  LIKE '%L/T%WWE-RAW%'         OR 
            src_series  LIKE 'L/T%WWE%SMACKDOWN%'    OR 
            (src_series LIKE 'L/T%WWE-NXT%' AND src_weekday <> 'Mon')
        )
GROUP BY 1,2,3,4,5