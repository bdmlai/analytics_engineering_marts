

  create view "entdwdb"."fds_kntr"."vw_aggr_kntr_schedule_wca_data__dbt_tmp" as (
    
select week_start_date, src_country, region, broadcast_network_prem_type, src_channel, demographic_type, 
demographic_group_name, series_name, series_type, hd_flag, channel_1, program_1, sum(duration_mins/60.00) as duration_hours,
(sum(rat_value*duration_mins))/(nullif(sum(nvl2(rat_value,duration_mins,null)),0)) as rat_value,
sum(viewing_hours) as viewing_hours, sum(aud) as weekly_cumulative_audience, count(*) as telecasts_count
from "entdwdb"."fds_kntr"."rpt_kntr_schedule_vh_data"
group by 1,2,3,4,5,6,7,8,9,10,11,12
  ) ;
