{{
  config({
        
		"materialized": 'ephemeral'
  })
}}

SELECT  src_country,
        rpt_yr,
        rpt_mn,
        broadcast_network_prem_type,
        'Combined RAW,SD,NXT and PPV'   AS series_name,
        'N'                             AS live_flag,
        sum(telecast_hours)             AS telecast_hours,
        sum(viewer_hours)               AS viewer_hours
FROM
(  
    (
        SELECT  src_country,
                extract (year from week_end_date)                           AS rpt_yr,
                extract (month from week_end_date)                          AS rpt_mn,
                broadcast_network_prem_type,
                Sum(CASE WHEN hd_flag = 'No' THEN duration_mins END)/60     AS telecast_hours,
                Sum(watched_mins)/60                                        AS viewer_hours
        FROM    {{ref('intm_kntr_monthly_india_tv_ppv')}}            
        GROUP BY 1,2,3,4
    )
    UNION ALL
    (
        SELECT  src_country,
                extract(year FROM dateadd(day,6,week_start_date))             AS rpt_yr,
                extract(month FROM dateadd(day,6,week_start_date))            AS rpt_mn,
                broadcast_network_prem_type,
                (Sum(CASE WHEN hd_flag = 'No' THEN  duration_mins END)/60)    AS telecast_hours,
                Sum(watched_mins)/60                                          AS viewer_hours
        FROM    {{source('fds_kntr','fact_kntr_wwe_telecast_data')}}
        WHERE   broadcast_date >= '2019-01-01'      AND
                src_country = 'india'               AND
                src_demographic_group = 'Everyone'  AND
                series_name in ('RAW','SmackDown','NXT')
        GROUP BY 1,2,3,4
    )
)
GROUP BY 1,2,3,4,5,6