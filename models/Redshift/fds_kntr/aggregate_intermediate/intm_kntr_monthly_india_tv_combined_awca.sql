{{
  config({
        
		"materialized": 'ephemeral'
  })
}}

SELECT  src_country,
        week_start_date,
        week_end_date,
        broadcast_network_prem_type,
        sum(cum_aud) AS cum_aud
FROM
(
    (
        SELECT  src_country,
                broadcast_network_prem_type,
                week_start_date,
                week_end_date,
                Sum(aud)    AS cum_aud
        FROM    {{ref('intm_kntr_monthly_india_tv_ppv')}}
        GROUP BY 1,2,3,4    

    )
    UNION ALL
    (
        SELECT  src_country,
                broadcast_network_prem_type,
                week_start_date,
                Dateadd(day,6,week_start_date) AS week_end_date,
                Sum(weekly_cumulative_audience) AS cum_aud
        FROM    {{source('fds_kntr','vw_aggr_kntr_schedule_wca_data')}}
        WHERE   demographic_group_name = 'Everyone'     AND
                week_start_date >= '2019-01-01'         AND 
                src_country = 'india'                   AND
                series_name in ('RAW','SmackDown','NXT')
        GROUP BY 1,2,3,4
    )
)
GROUP BY 1,2,3,4
