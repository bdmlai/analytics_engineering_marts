{{
  config({
        
		"materialized": 'ephemeral'
  })
}}

SELECT  a.*,
        b.ppv_start_date,
        b.ppv_end_date,
        dateadd(day,6,a.week_start_date) AS week_end_date
FROM
    (
        (   
            SELECT * FROM {{source('fds_kntr','fact_kntr_wwe_telecast_data')}} 
        ) a
        JOIN
        (   
            SELECT  event_name,
                    trunc(dateadd(day,1,event_date))    AS ppv_start_date, 
                    trunc(dateadd(day,8,event_date))    AS ppv_end_date 
            FROM    {{source('fds_nplus','rpt_network_ppv_liveplusvod')}}
            WHERE   event_date >= '2019-01-01'  AND 
                    data_level = 'Live'         AND 
                    platform = 'Total'
        ) b
        ON  a.broadcast_date >= b.ppv_start_date      AND
            a.broadcast_date <= b.ppv_end_date        AND
            lower(a.series_episode_name) LIKE ('%'+lower(b.event_name)+'%')
    )    
WHERE   broadcast_date >= '2019-01-01'      AND
        src_country = 'india'               AND
        src_demographic_group = 'Everyone'  AND 
        a.series_name = 'PPV'   
    



