CREATE VIEW VW_AGGR_KNTR_MONTHLY_WWE_PROGRAM_RATING_SCHEDULE
AS
select  TO_CHAR(TO_DATE (cal_month::text, 'MM'), 'Mon') as month,
        cal_year as year,
        src_country,
        src_channel,
        series_name,
        src_demographic_group ,
        src_demographic_age ,
        hd_flag ,
        live_flag,
        nth_run,
		(sum(rat_value*total_duration_mins))/(nullif(sum(nvl2(rat_value,total_duration_mins,null)),0)) as rat_value,
        sum(viewing_hours) as viewing_hours,
        sum(duration_hours) as duration_hours,
        sum(count_telecast) as count_telecast,
        avg(Sum_Weekly_Cumulative_Audience) as average_weekly_cumulative_audience_000
        
from
(
select  week_Start_date,
        extract(month from week_Start_date) as cal_month,
        extract(quarter from week_Start_date) as cal_quarter,
        extract(year from week_Start_date) as cal_year,
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
from fact_kntr_wwe_telecast_data a
where a.series_name in ('SmackDown','RAW','NXT','PPV')
  and a.nth_run is not null
group by 1,2,3,4,5,6,7,8,9,10,11,12
)
group by 1,2,3,4,5,6,7,8,9,10;