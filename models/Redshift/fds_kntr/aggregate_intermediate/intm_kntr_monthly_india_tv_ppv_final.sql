{{
  config({
        
		"materialized": 'ephemeral'
  })
}}

SELECT  a.src_country,
        a.broadcast_network_prem_type,
        a.rpt_yr,
        a.rpt_mn, 
        a.AWCA,
        b.telecast_hours,
        b.viewer_hours
FROM
    (
        (   
            SELECT  src_country,
                    broadcast_network_prem_type,
                    extract (year from week_end_date)   AS rpt_yr,
                    extract (month from week_end_date)  AS rpt_mn,
                    Avg(sum_aud)                        AS AWCA
            FROM
            (
                SELECT  src_country,
                        broadcast_network_prem_type,
                        week_start_date,
                        week_end_date,
                        Sum(aud)    AS sum_aud
                FROM    {{ref('intm_kntr_monthly_india_tv_ppv')}}
                GROUP BY 1,2,3,4
            )
            GROUP BY 1,2,3,4
        ) a
        LEFT JOIN
        (
            SELECT  src_country,
                    broadcast_network_prem_type,
                    extract (year from week_end_date)                           AS rpt_yr,
                    extract (month from week_end_date)                          AS rpt_mn,
                    Sum(CASE WHEN hd_flag = 'No' THEN duration_mins END)/60     AS telecast_hours,
                    Sum(watched_mins)/60                                        AS viewer_hours
            FROM    {{ref('intm_kntr_monthly_india_tv_ppv')}}            
            GROUP BY 1,2,3,4
        ) b
        ON  a.src_country = b.src_country                                   AND 
            a.broadcast_network_prem_type = b.broadcast_network_prem_type   AND
            a.rpt_yr = b.rpt_yr                                             AND
            a.rpt_mn = b.rpt_mn 
    )
