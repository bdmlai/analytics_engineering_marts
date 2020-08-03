
select dim_date_id, broadcast_date, src_country, region, broadcast_network_prem_type, src_channel, demographic_type, 
demographic_group_name, series_name, series_type, start_time, end_time, duration_mins, hd_flag, start_time_modified,
channel_1, program_1, rat_value, viewing_hours, regional_viewing_hours
from "entdwdb"."fds_kntr"."rpt_kntr_schedule_vh_data"