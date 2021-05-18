{{
  config({
        
		"materialized": 'ephemeral'
  })
}}

SELECT  a.src_country,
        a.series_name,
        a.broadcast_network_prem_type,
        a.rpt_yr,
        a.rpt_mn,
        a.live_flag,
        a.AWCA,
        b.telecast_hours,
        b.viewer_hours

FROM
    (
        (
            (
                SELECT  src_country,
                        broadcast_network_prem_type,
                        series_name,
                        extract(year FROM week_end_date)             AS rpt_yr,
                        extract(month FROM week_end_date)            AS rpt_mn,
                        Avg(aud_sum)                                 AS AWCA, 
                        'Y'                                          AS live_flag
                FROM    {{ref('intm_kntr_monthly_india_tv_live_awca')}}
                GROUP BY 1,2,3,4,5
            )
            UNION ALL
            (
                SELECT  src_country,
                        broadcast_network_prem_type,
                        'All WWE'                           AS series_name,
                        extract (year from week_end_date)   AS rpt_yr,
                        extract (month from week_end_date)  AS rpt_mn,
                        avg(cum_aud)                        AS AWCA,
                        'N'                                 AS live_flag
                FROM    {{ref('intm_kntr_monthly_india_tv_allwwe_awca')}}
                GROUP BY 1,2,4,5
            )
            UNION ALL
            (
                SELECT  src_country,
                        broadcast_network_prem_type,
                        series_name,
                        extract (year from week_end_date)   AS rpt_yr,
                        extract (month from week_end_date)  AS rpt_mn,
                        avg(cum_aud)                        AS AWCA,
                        'N'                                 AS live_flag 
                FROM    {{ref('intm_kntr_monthly_india_tv_awca')}}
                GROUP BY 1,2,3,4,5 
            )
            UNION ALL
            (
                SELECT  src_country,
                        broadcast_network_prem_type,
                        'Combined RAW,SD,NXT and PPV'       AS series_name,
                        extract (year from week_end_date)   AS rpt_yr,
                        extract (month from week_end_date)  AS rpt_mn,
                        avg(cum_aud)                        AS AWCA,
                        'N'                                 AS live_flag 
                FROM    {{ref('intm_kntr_monthly_india_tv_combined_awca')}}
                GROUP BY 1,2,3,4,5 
            )
        ) a
        LEFT JOIN
        (
            (
                SELECT * FROM {{ref('intm_kntr_monthly_india_tv_th')}}
            )
            UNION ALL
            (
                SELECT * FROM {{ref('intm_kntr_monthly_india_tv_allwwe_th')}}
            )
            UNION ALL
            (
                SELECT * FROM {{ref('intm_kntr_monthly_india_tv_combined_th')}}
            )
        ) b
        ON  a.series_name = b.series_name                                   AND
            a.broadcast_network_prem_type = b.broadcast_network_prem_type   AND
            a.src_country = b.src_country                                   AND
            a.rpt_yr = b.rpt_yr                                             AND
            a.rpt_mn = b.rpt_mn                                             AND
            a.series_name = b.series_name                                   AND
            a.live_flag = b.live_flag
    )