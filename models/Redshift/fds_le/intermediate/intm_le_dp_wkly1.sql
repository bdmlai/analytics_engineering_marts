{{ config({ "schema": 'fds_le', "materialized": 'ephemeral',"tags": 'rpt_le_weekly_consolidated_kpi' }) }}
select
          a.*
        , a.cal_year_week_num_mon                               as week
        , b.cal_year                                            as prev_cal_year
        , b.cal_year_week_num_mon                               as prev_cal_year_week_num_mon
        , b.cal_year_mon_week_begin_date                        as prev_cal_year_mon_week_begin_date
        , b.cal_year_mon_week_end_date                          as prev_cal_year_mon_week_end_date
        , coalesce(b.no_of_total_events_wk,0)                   as prev_no_of_total_events_wk
        , coalesce(b.no_of_house_events_wk,0)                   as prev_no_of_house_events_wk
        , coalesce(b.no_of_raw_house_events_wk,0)               as prev_no_of_raw_house_events_wk
        , coalesce(b.no_of_smackdown_house_events_wk,0)         as prev_no_of_smackdown_house_events_wk
        , coalesce(b.no_of_combined_house_events_wk,0)          as prev_no_of_combined_house_events_wk
        , coalesce(b.no_of_tv_events_wk,0)                      as prev_no_of_tv_events_wk
        , coalesce(b.no_of_raw_tv_events_wk,0)                  as prev_no_of_raw_tv_events_wk
        , coalesce(b.no_of_smackdown_tv_events_wk,0)            as prev_no_of_smackdown_tv_events_wk
        , coalesce(b.no_of_combined_tv_events_wk,0)             as prev_no_of_combined_tv_events_wk
        , coalesce(b.no_of_ppv_events_wk,0)                     as prev_no_of_ppv_events_wk
        , coalesce(b.total_paid_attendance_wk,0)                as prev_total_paid_attendance_wk
        , coalesce(b.capacity_wk,0)                             as prev_capacity_wk
        , coalesce(b.total_paid_utilization_wk,0)               as prev_total_paid_utilization_wk
        , coalesce(b.avg_total_attendance_wk,0)                 as prev_avg_total_attendance_wk
        , coalesce(b.avg_house_event_attendance_wk,0)           as prev_avg_house_event_attendance_wk
        , coalesce(b.avg_raw_house_event_attendance_wk,0)       as prev_avg_raw_house_event_attendance_wk
        , coalesce(b.avg_smackdown_house_event_attendance_wk,0) as prev_avg_smackdown_house_event_attendance_wk
        , coalesce(b.avg_cmb_house_event_attendance_wk,0)       as prev_avg_cmb_house_event_attendance_wk
        , coalesce(b.avg_tv_event_attendance_wk,0)              as prev_avg_tv_event_attendance_wk
        , coalesce(b.avg_raw_tv_event_attendance_wk,0)          as prev_avg_raw_tv_event_attendance_wk
        , coalesce(b.avg_smackdown_tv_event_attendance_wk,0)    as prev_avg_smackdown_tv_event_attendance_wk
        , coalesce(b.avg_cmb_tv_event_attendance_wk,0)          as prev_avg_cmb_tv_event_attendance_wk
        , coalesce(b.avg_ppv_event_attendance_wk,0)             as prev_avg_ppv_event_attendance_wk
from
          {{ref('intm_le_dp_wkly')}} a
          left join
                    {{ref('intm_le_dp_wkly')}} b
                    on
                              (
                                        a.cal_year-1
                              )
                                                          = b.cal_year
                              and a.cal_year_week_num_mon = b.cal_year_week_num_mon