CREATE VIEW VW_AGGR_KNTR_QUARTERLY_WWE_PROGRAM_RATING
AS
select 'q'+ cast(quarter as varchar) as quarter,
year,src_country,src_channel,
series_name,src_demographic_group,src_demographic_age,hd_flag ,
(sum(rat_value*total_duration_mins))/(nullif(sum(nvl2(rat_value,total_duration_mins,null)),0)) as rat_value,
sum(viewing_hours) as viewing_hours,
sum(duration_hours) as duration_hours,
sum(count_telecast) as count_telecast,
avg(Weekly_Cumulative_Audience) as average_weekly_cumulative_audience_000
from 
(
select  week_start_date,
 extract(month from week_Start_date)  as month,
        extract(quarter from week_Start_date) as quarter,
        extract(year from week_Start_date) as year,
src_country,src_channel,
series_name,src_demographic_group,src_demographic_age,hd_flag ,
(sum(rat_value*duration_mins))/(nullif(sum(nvl2(rat_value,duration_mins,null)),0)) as rat_value,
sum(duration_mins) as total_duration_mins,
sum(watched_mins/60) as viewing_hours,
sum(duration_mins/60.00) as duration_hours,
count(*) as count_telecast,
sum(aud/1000) as Weekly_Cumulative_Audience
from fact_kntr_wwe_telecast_data a
group by 1,2,3,4,5,6,7,8,9,10
) group by 1,2,3,4,5,6,7,8;