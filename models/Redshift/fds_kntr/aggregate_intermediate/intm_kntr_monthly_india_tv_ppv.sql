{{
  config({
        
		"materialized": 'ephemeral'
  })
}}

SELECT  a.src_country,
        a.src_demographic_group,
        a.broadcast_date,
        a.series_name,
        a.series_episode_name,
        a.broadcast_network_prem_type,
        a.live_flag,
        a.hd_flag,
        a.aud,
        a.duration_mins,
        a.week_start_date,
        a.watched_mins,
        b.ppv_start_date,
        b.ppv_end_date,
        dateadd(day,6,a.week_start_date) AS week_end_date
FROM
    (
        (   
            SELECT  src_country,
                    src_demographic_group,
                    broadcast_date,
                    series_name,
                    series_episode_name,
                    broadcast_network_prem_type,
                    live_flag,
                    hd_flag,
                    aud,
                    duration_mins,
                    week_start_date,
                    watched_mins
            FROM    {{source('fds_kntr','fact_kntr_wwe_telecast_data')}} 
        ) a
        JOIN
        (   
            SELECT  ppv_nm,
                    trunc(dateadd(day,1,event_dttm))    AS ppv_start_date, 
                    trunc(dateadd(day,8,event_dttm))    AS ppv_end_date 
            FROM    {{source('cdm','dim_event')}}
            WHERE   event_dttm >= '2019-01-01'  AND 
                    ppv_nm <> ' '               AND
                    cancelled_by IS NULL        
        ) b
        ON  a.broadcast_date >= b.ppv_start_date      AND
            a.broadcast_date <= b.ppv_end_date        AND
            lower(replace(series_episode_name, ' ', '')) = lower(replace(ppv_nm, ' ', '')) 
    )    
WHERE   broadcast_date >= '2019-01-01'      AND
        src_country = 'india'               AND
        src_demographic_group = 'Everyone'  AND 
        a.series_name = 'PPV'   
    



