CREATE VIEW VW_AGGR_KNTR_YEARLY_COMPETITIVE_PROGRAM_RATINGS
AS
select year, src_country, src_channel, src_property, demographic, hd_flag, sum(duration_hours) as duration_hours,
(sum(rat_value * total_duration_mins))/(nullif(sum(nvl2(rat_value, total_duration_mins, null)),0)) as rat_value,
sum(viewing_hours) as viewing_hours, sum(telecasts_count) as telecasts_count,
avg(weekly_cumulative_audience) as average_weekly_cumulative_audience_000
from aggr_kntr_weekly_competitive_program_ratings
group by 1,2,3,4,5,6;