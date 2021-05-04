{{ config({ "schema": 'fds_le', "materialized": 'ephemeral',"tags": 'rpt_le_weekly_consolidated_kpi' }) }}
select *
from
       (
              select
                     'Weekly' as granularity
                   , platform
                   , 'Number of Total Events' as Metric
                   , week
                   , cal_year
                   , cal_mth_num
                   , cal_year_week_num_mon
                   , cal_year_mon_week_begin_date
                   , cal_year_mon_week_end_date
                   , no_of_total_events_wk as value
                   , prev_cal_year
                   , prev_cal_year_week_num_mon
                   , prev_cal_year_mon_week_begin_date
                   , prev_cal_year_mon_week_end_date
                   , prev_no_of_total_events_wk as prev_year_value
              from
                     {{ref('intm_le_dp_wkly1')}}
              union all
              select
                     'Weekly' as granularity
                   , platform
                   , 'Number of House Events' as Metric
                   , week
                   , cal_year
                   , cal_mth_num
                   , cal_year_week_num_mon
                   , cal_year_mon_week_begin_date
                   , cal_year_mon_week_end_date
                   , no_of_house_events_wk as value
                   , prev_cal_year
                   , prev_cal_year_week_num_mon
                   , prev_cal_year_mon_week_begin_date
                   , prev_cal_year_mon_week_end_date
                   , prev_no_of_house_events_wk as prev_year_value
              from
                     {{ref('intm_le_dp_wkly1')}}
              union all
              select
                     'Weekly' as granularity
                   , platform
                   , 'Number of Raw House Events' as Metric
                   , week
                   , cal_year
                   , cal_mth_num
                   , cal_year_week_num_mon
                   , cal_year_mon_week_begin_date
                   , cal_year_mon_week_end_date
                   , no_of_raw_house_events_wk as value
                   , prev_cal_year
                   , prev_cal_year_week_num_mon
                   , prev_cal_year_mon_week_begin_date
                   , prev_cal_year_mon_week_end_date
                   , prev_no_of_raw_house_events_wk as prev_year_value
              from
                     {{ref('intm_le_dp_wkly1')}}
              union all
              select
                     'Weekly' as granularity
                   , platform
                   , 'Number of Smackdown House Events' as Metric
                   , week
                   , cal_year
                   , cal_mth_num
                   , cal_year_week_num_mon
                   , cal_year_mon_week_begin_date
                   , cal_year_mon_week_end_date
                   , no_of_smackdown_house_events_wk as value
                   , prev_cal_year
                   , prev_cal_year_week_num_mon
                   , prev_cal_year_mon_week_begin_date
                   , prev_cal_year_mon_week_end_date
                   , prev_no_of_smackdown_house_events_wk as prev_year_value
              from
                     {{ref('intm_le_dp_wkly1')}}
              union all
              select
                     'Weekly' as granularity
                   , platform
                   , 'Number of Combined House Events' as Metric
                   , week
                   , cal_year
                   , cal_mth_num
                   , cal_year_week_num_mon
                   , cal_year_mon_week_begin_date
                   , cal_year_mon_week_end_date
                   , no_of_combined_house_events_wk as value
                   , prev_cal_year
                   , prev_cal_year_week_num_mon
                   , prev_cal_year_mon_week_begin_date
                   , prev_cal_year_mon_week_end_date
                   , prev_no_of_combined_house_events_wk as prev_year_value
              from
                     {{ref('intm_le_dp_wkly1')}}
              union all
              select
                     'Weekly' as granularity
                   , platform
                   , 'Number of TV Events' as Metric
                   , week
                   , cal_year
                   , cal_mth_num
                   , cal_year_week_num_mon
                   , cal_year_mon_week_begin_date
                   , cal_year_mon_week_end_date
                   , no_of_tv_events_wk as value
                   , prev_cal_year
                   , prev_cal_year_week_num_mon
                   , prev_cal_year_mon_week_begin_date
                   , prev_cal_year_mon_week_end_date
                   , prev_no_of_tv_events_wk as prev_year_value
              from
                     {{ref('intm_le_dp_wkly1')}}
              union all
              select
                     'Weekly' as granularity
                   , platform
                   , 'Number of Raw TV Events' as Metric
                   , week
                   , cal_year
                   , cal_mth_num
                   , cal_year_week_num_mon
                   , cal_year_mon_week_begin_date
                   , cal_year_mon_week_end_date
                   , no_of_raw_tv_events_wk as value
                   , prev_cal_year
                   , prev_cal_year_week_num_mon
                   , prev_cal_year_mon_week_begin_date
                   , prev_cal_year_mon_week_end_date
                   , prev_no_of_raw_tv_events_wk as prev_year_value
              from
                     {{ref('intm_le_dp_wkly1')}}
              union all
              select
                     'Weekly' as granularity
                   , platform
                   , 'Number of Smackdown TV Events' as Metric
                   , week
                   , cal_year
                   , cal_mth_num
                   , cal_year_week_num_mon
                   , cal_year_mon_week_begin_date
                   , cal_year_mon_week_end_date
                   , no_of_smackdown_tv_events_wk as value
                   , prev_cal_year
                   , prev_cal_year_week_num_mon
                   , prev_cal_year_mon_week_begin_date
                   , prev_cal_year_mon_week_end_date
                   , prev_no_of_smackdown_tv_events_wk as prev_year_value
              from
                     {{ref('intm_le_dp_wkly1')}}
              union all
              select
                     'Weekly' as granularity
                   , platform
                   , 'Number of Combined TV Events' as Metric
                   , week
                   , cal_year
                   , cal_mth_num
                   , cal_year_week_num_mon
                   , cal_year_mon_week_begin_date
                   , cal_year_mon_week_end_date
                   , no_of_combined_tv_events_wk as value
                   , prev_cal_year
                   , prev_cal_year_week_num_mon
                   , prev_cal_year_mon_week_begin_date
                   , prev_cal_year_mon_week_end_date
                   , prev_no_of_combined_tv_events_wk as prev_year_value
              from
                     {{ref('intm_le_dp_wkly1')}}
              union all
              select
                     'Weekly' as granularity
                   , platform
                   , 'Number of PPV Events' as Metric
                   , week
                   , cal_year
                   , cal_mth_num
                   , cal_year_week_num_mon
                   , cal_year_mon_week_begin_date
                   , cal_year_mon_week_end_date
                   , no_of_ppv_events_wk as value
                   , prev_cal_year
                   , prev_cal_year_week_num_mon
                   , prev_cal_year_mon_week_begin_date
                   , prev_cal_year_mon_week_end_date
                   , prev_no_of_ppv_events_wk as prev_year_value
              from
                     {{ref('intm_le_dp_wkly1')}}
              union all
              select
                     'Weekly' as granularity
                   , platform
                   , 'Total Paid Attendance' as Metric
                   , week
                   , cal_year
                   , cal_mth_num
                   , cal_year_week_num_mon
                   , cal_year_mon_week_begin_date
                   , cal_year_mon_week_end_date
                   , total_paid_attendance_wk as value
                   , prev_cal_year
                   , prev_cal_year_week_num_mon
                   , prev_cal_year_mon_week_begin_date
                   , prev_cal_year_mon_week_end_date
                   , prev_total_paid_attendance_wk as prev_year_value
              from
                     {{ref('intm_le_dp_wkly1')}}
              union all
              select
                     'Weekly' as granularity
                   , platform
                   , 'Capacity' as Metric
                   , week
                   , cal_year
                   , cal_mth_num
                   , cal_year_week_num_mon
                   , cal_year_mon_week_begin_date
                   , cal_year_mon_week_end_date
                   , capacity_wk as value
                   , prev_cal_year
                   , prev_cal_year_week_num_mon
                   , prev_cal_year_mon_week_begin_date
                   , prev_cal_year_mon_week_end_date
                   , prev_capacity_wk as prev_year_value
              from
                     {{ref('intm_le_dp_wkly1')}}
              union all
              select
                     'Weekly' as granularity
                   , platform
                   , 'Total Paid Utilization' as Metric
                   , week
                   , cal_year
                   , cal_mth_num
                   , cal_year_week_num_mon
                   , cal_year_mon_week_begin_date
                   , cal_year_mon_week_end_date
                   , total_paid_utilization_wk as value
                   , prev_cal_year
                   , prev_cal_year_week_num_mon
                   , prev_cal_year_mon_week_begin_date
                   , prev_cal_year_mon_week_end_date
                   , prev_total_paid_utilization_wk as prev_year_value
              from
                     {{ref('intm_le_dp_wkly1')}}
              union all
              select
                     'Weekly' as granularity
                   , platform
                   , 'Total Paid Utilization New' as Metric
                   , week
                   , cal_year
                   , cal_mth_num
                   , cal_year_week_num_mon
                   , cal_year_mon_week_begin_date
                   , cal_year_mon_week_end_date
                   , total_paid_attendance_wk/nullif(capacity_wk,0) as value
                   , prev_cal_year
                   , prev_cal_year_week_num_mon
                   , prev_cal_year_mon_week_begin_date
                   , prev_cal_year_mon_week_end_date
                   , prev_total_paid_attendance_wk/nullif(prev_capacity_wk,0) as prev_year_value
              from
                     {{ref('intm_le_dp_wkly1')}}
              union all
              select
                     'Weekly' as granularity
                   , platform
                   , 'Average Total Attendance' as Metric
                   , week
                   , cal_year
                   , cal_mth_num
                   , cal_year_week_num_mon
                   , cal_year_mon_week_begin_date
                   , cal_year_mon_week_end_date
                   , avg_total_attendance_wk as value
                   , prev_cal_year
                   , prev_cal_year_week_num_mon
                   , prev_cal_year_mon_week_begin_date
                   , prev_cal_year_mon_week_end_date
                   , prev_avg_total_attendance_wk as prev_year_value
              from
                     {{ref('intm_le_dp_wkly1')}}
              union all
              select
                     'Weekly' as granularity
                   , platform
                   , 'Average House Event Attendance' as Metric
                   , week
                   , cal_year
                   , cal_mth_num
                   , cal_year_week_num_mon
                   , cal_year_mon_week_begin_date
                   , cal_year_mon_week_end_date
                   , avg_house_event_attendance_wk as value
                   , prev_cal_year
                   , prev_cal_year_week_num_mon
                   , prev_cal_year_mon_week_begin_date
                   , prev_cal_year_mon_week_end_date
                   , prev_avg_house_event_attendance_wk as prev_year_value
              from
                     {{ref('intm_le_dp_wkly1')}}
              union all
              select
                     'Weekly' as granularity
                   , platform
                   , 'Average Raw House Event Attendance' as Metric
                   , week
                   , cal_year
                   , cal_mth_num
                   , cal_year_week_num_mon
                   , cal_year_mon_week_begin_date
                   , cal_year_mon_week_end_date
                   , avg_raw_house_event_attendance_wk as value
                   , prev_cal_year
                   , prev_cal_year_week_num_mon
                   , prev_cal_year_mon_week_begin_date
                   , prev_cal_year_mon_week_end_date
                   , prev_avg_raw_house_event_attendance_wk as prev_year_value
              from
                     {{ref('intm_le_dp_wkly1')}}
              union all
              select
                     'Weekly' as granularity
                   , platform
                   , 'Average Smackdown House Event Attendance' as Metric
                   , week
                   , cal_year
                   , cal_mth_num
                   , cal_year_week_num_mon
                   , cal_year_mon_week_begin_date
                   , cal_year_mon_week_end_date
                   , avg_smackdown_house_event_attendance_wk as value
                   , prev_cal_year
                   , prev_cal_year_week_num_mon
                   , prev_cal_year_mon_week_begin_date
                   , prev_cal_year_mon_week_end_date
                   , prev_avg_smackdown_house_event_attendance_wk as prev_year_value
              from
                     {{ref('intm_le_dp_wkly1')}}
              union all
              select
                     'Weekly' as granularity
                   , platform
                   , 'Average CMB House Event Attendance' as Metric
                   , week
                   , cal_year
                   , cal_mth_num
                   , cal_year_week_num_mon
                   , cal_year_mon_week_begin_date
                   , cal_year_mon_week_end_date
                   , avg_cmb_house_event_attendance_wk as value
                   , prev_cal_year
                   , prev_cal_year_week_num_mon
                   , prev_cal_year_mon_week_begin_date
                   , prev_cal_year_mon_week_end_date
                   , prev_avg_cmb_house_event_attendance_wk as prev_year_value
              from
                     {{ref('intm_le_dp_wkly1')}}
              union all
              select
                     'Weekly' as granularity
                   , platform
                   , 'Average TV Event Attendance' as Metric
                   , week
                   , cal_year
                   , cal_mth_num
                   , cal_year_week_num_mon
                   , cal_year_mon_week_begin_date
                   , cal_year_mon_week_end_date
                   , avg_tv_event_attendance_wk as value
                   , prev_cal_year
                   , prev_cal_year_week_num_mon
                   , prev_cal_year_mon_week_begin_date
                   , prev_cal_year_mon_week_end_date
                   , prev_avg_tv_event_attendance_wk as prev_year_value
              from
                     {{ref('intm_le_dp_wkly1')}}
              union all
              select
                     'Weekly' as granularity
                   , platform
                   , 'Average Raw TV Event Attendance' as Metric
                   , week
                   , cal_year
                   , cal_mth_num
                   , cal_year_week_num_mon
                   , cal_year_mon_week_begin_date
                   , cal_year_mon_week_end_date
                   , avg_raw_tv_event_attendance_wk as value
                   , prev_cal_year
                   , prev_cal_year_week_num_mon
                   , prev_cal_year_mon_week_begin_date
                   , prev_cal_year_mon_week_end_date
                   , prev_avg_raw_tv_event_attendance_wk as prev_year_value
              from
                     {{ref('intm_le_dp_wkly1')}}
              union all
              select
                     'Weekly' as granularity
                   , platform
                   , 'Average Smackdown TV Event Attendance' as Metric
                   , week
                   , cal_year
                   , cal_mth_num
                   , cal_year_week_num_mon
                   , cal_year_mon_week_begin_date
                   , cal_year_mon_week_end_date
                   , avg_smackdown_tv_event_attendance_wk as value
                   , prev_cal_year
                   , prev_cal_year_week_num_mon
                   , prev_cal_year_mon_week_begin_date
                   , prev_cal_year_mon_week_end_date
                   , prev_avg_smackdown_tv_event_attendance_wk as prev_year_value
              from
                     {{ref('intm_le_dp_wkly1')}}
              union all
              select
                     'Weekly' as granularity
                   , platform
                   , 'Average CMB TV Event Attendance' as Metric
                   , week
                   , cal_year
                   , cal_mth_num
                   , cal_year_week_num_mon
                   , cal_year_mon_week_begin_date
                   , cal_year_mon_week_end_date
                   , avg_cmb_tv_event_attendance_wk as value
                   , prev_cal_year
                   , prev_cal_year_week_num_mon
                   , prev_cal_year_mon_week_begin_date
                   , prev_cal_year_mon_week_end_date
                   , prev_avg_cmb_tv_event_attendance_wk as prev_year_value
              from
                     {{ref('intm_le_dp_wkly1')}}
              union all
              select
                     'Weekly' as granularity
                   , platform
                   , 'Average PPV Event Attendance' as Metric
                   , week
                   , cal_year
                   , cal_mth_num
                   , cal_year_week_num_mon
                   , cal_year_mon_week_begin_date
                   , cal_year_mon_week_end_date
                   , avg_ppv_event_attendance_wk as value
                   , prev_cal_year
                   , prev_cal_year_week_num_mon
                   , prev_cal_year_mon_week_begin_date
                   , prev_cal_year_mon_week_end_date
                   , prev_avg_ppv_event_attendance_wk as prev_year_value
              from
                     {{ref('intm_le_dp_wkly1')}}
       )