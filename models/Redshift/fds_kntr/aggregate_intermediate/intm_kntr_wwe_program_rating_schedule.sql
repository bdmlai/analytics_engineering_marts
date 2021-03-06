
{{
  config({
		'schema': 'fds_kntr',
		"materialized": 'ephemeral'
  })
}}

select  week_Start_date,
        extract(month from week_Start_date+6) as cal_month,
        extract(quarter from week_Start_date+6) as cal_quarter,
        extract(year from week_Start_date+6) as cal_year,
        src_country,
        src_channel,
        series_name,
        src_demographic_group,
        src_demographic_age,
        hd_flag ,
        live_flag,
        nth_run,
        (sum(rat_value*duration_mins))/(nullif(sum(nvl2(rat_value,duration_mins,null)),0)) as rat_value,
        sum(duration_mins) as total_duration_mins,
        sum(watched_mins/60) as viewing_hours,
        sum(duration_mins/60.00) as duration_hours,
        sum(aud/1000) as Sum_Weekly_Cumulative_Audience,
        count(*) as count_telecast


from {{source('fds_kntr','fact_kntr_wwe_telecast_data')}} a
where a.series_name in ('SmackDown','RAW','NXT','PPV')
  and a.nth_run is not null
 group by 1,2,3,4,5,6,7,8,9,10,11,12
