{{ config({ "materialized": 'ephemeral' }) }}
SELECT t1.*,
         t2.filter_name AS audience_type,
        t2.plays_upp AS concurrent_plays
FROM {{ref('intm_nplus_aa_impressions_raweventlog_encompass_join_time')}} t1
LEFT JOIN 
    (SELECT filter_name,
        concurrent_plays,
        ts2,
         lead(concurrent_plays)
        OVER (partition by filter_name
    ORDER BY  ts2) AS plays_upp, lead(ts2)
        OVER (partition by filter_name
    ORDER BY  ts2) AS ts2_upp
    FROM 
        (SELECT *
        FROM 
            (SELECT * ,
         pulse_timestamp AS ts2 ,
         rank() over( partition by pulse_timestamp,
        filter_name
            ORDER BY  as_on_date DESC ) AS rank
            FROM {{source('hive_udl_ads','conviva_daily_pulse')}}
            WHERE pulse_timestamp::date >= '2017-01-01' )
            WHERE rank = 1 ) ) t2
            ON t1.end_time> t2.ts2
            AND t1.end_time<=t2.ts2_upp 