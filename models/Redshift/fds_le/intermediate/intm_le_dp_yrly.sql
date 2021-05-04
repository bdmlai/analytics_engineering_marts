{{ config({ "schema": 'fds_le', "materialized": 'ephemeral',"tags": 'rpt_le_weekly_consolidated_kpi' }) }}
select
          a.platform
        , a.cal_year
        , a.cal_mth_num
        , a.cal_year_week_num_mon
        , a.cal_year_mon_week_begin_date
        , a.cal_year_mon_week_end_date
        , a.cal_year_week_num_mon as week
        , a.prev_cal_year
        , a.prev_cal_year_week_num_mon
        , a.prev_cal_year_mon_week_begin_date
        , a.prev_cal_year_mon_week_end_date
        , sum(b.no_of_total_events_wk)                        as no_of_total_events_ytd
        , sum(b.no_of_house_events_wk)                        as no_of_house_events_ytd
        , sum(b.no_of_raw_house_events_wk)                    as no_of_raw_house_events_ytd
        , sum(b.no_of_smackdown_house_events_wk)              as no_of_smackdown_house_events_ytd
        , sum(b.no_of_combined_house_events_wk)               as no_of_combined_house_events_ytd
        , sum(b.no_of_tv_events_wk)                           as no_of_tv_events_ytd
        , sum(b.no_of_raw_tv_events_wk)                       as no_of_raw_tv_events_ytd
        , sum(b.no_of_smackdown_tv_events_wk)                 as no_of_smackdown_tv_events_ytd
        , sum(b.no_of_combined_tv_events_wk)                  as no_of_combined_tv_events_ytd
        , sum(b.no_of_ppv_events_wk)                          as no_of_ppv_events_ytd
        , sum(b.total_paid_attendance_wk)                     as total_paid_attendance_ytd
        , sum(b.capacity_wk)                                  as capacity_ytd
        , avg(b.total_paid_utilization_wk)                    as total_paid_utilization_ytd
        , avg(b.avg_total_attendance_wk)                      as avg_total_attendance_ytd
        , avg(b.avg_house_event_attendance_wk)                as avg_house_event_attendance_ytd
        , avg(b.avg_raw_house_event_attendance_wk)            as avg_raw_house_event_attendance_ytd
        , avg(b.avg_smackdown_house_event_attendance_wk)      as avg_smackdown_house_event_attendance_ytd
        , avg(b.avg_cmb_house_event_attendance_wk)            as avg_cmb_house_event_attendance_ytd
        , avg(b.avg_tv_event_attendance_wk)                   as avg_tv_event_attendance_ytd
        , avg(b.avg_raw_tv_event_attendance_wk)               as avg_raw_tv_event_attendance_ytd
        , avg(b.avg_smackdown_tv_event_attendance_wk)         as avg_smackdown_tv_event_attendance_ytd
        , avg(b.avg_cmb_tv_event_attendance_wk)               as avg_cmb_tv_event_attendance_ytd
        , avg(b.avg_ppv_event_attendance_wk)                  as avg_ppv_event_attendance_ytd
        , sum(b.prev_no_of_total_events_wk)                   as prev_no_of_total_events_ytd
        , sum(b.prev_no_of_house_events_wk)                   as prev_no_of_house_events_ytd
        , sum(b.prev_no_of_raw_house_events_wk)               as prev_no_of_raw_house_events_ytd
        , sum(b.prev_no_of_smackdown_house_events_wk)         as prev_no_of_smackdown_house_events_ytd
        , sum(b.prev_no_of_combined_house_events_wk)          as prev_no_of_combined_house_events_ytd
        , sum(b.prev_no_of_tv_events_wk)                      as prev_no_of_tv_events_ytd
        , sum(b.prev_no_of_raw_tv_events_wk)                  as prev_no_of_raw_tv_events_ytd
        , sum(b.prev_no_of_smackdown_tv_events_wk)            as prev_no_of_smackdown_tv_events_ytd
        , sum(b.prev_no_of_combined_tv_events_wk)             as prev_no_of_combined_tv_events_ytd
        , sum(b.prev_no_of_ppv_events_wk)                     as prev_no_of_ppv_events_ytd
        , sum(b.prev_total_paid_attendance_wk)                as prev_total_paid_attendance_ytd
        , sum(b.prev_capacity_wk)                             as prev_capacity_ytd
        , avg(b.prev_total_paid_utilization_wk)               as prev_total_paid_utilization_ytd
        , avg(b.prev_avg_total_attendance_wk)                 as prev_avg_total_attendance_ytd
        , avg(b.prev_avg_house_event_attendance_wk)           as prev_avg_house_event_attendance_ytd
        , avg(b.prev_avg_raw_house_event_attendance_wk)       as prev_avg_raw_house_event_attendance_ytd
        , avg(b.prev_avg_smackdown_house_event_attendance_wk) as prev_avg_smackdown_house_event_attendance_ytd
        , avg(b.prev_avg_cmb_house_event_attendance_wk)       as prev_avg_cmb_house_event_attendance_ytd
        , avg(b.prev_avg_tv_event_attendance_wk)              as prev_avg_tv_event_attendance_ytd
        , avg(b.prev_avg_raw_tv_event_attendance_wk)          as prev_avg_raw_tv_event_attendance_ytd
        , avg(b.prev_avg_smackdown_tv_event_attendance_wk)    as prev_avg_smackdown_tv_event_attendance_ytd
        , avg(b.prev_avg_cmb_tv_event_attendance_wk)          as prev_avg_cmb_tv_event_attendance_ytd
        , avg(b.prev_avg_ppv_event_attendance_wk)             as prev_avg_ppv_event_attendance_ytd
from
          {{ref('intm_le_dp_wkly1')}} a
          left join
                    {{ref('intm_le_dp_wkly1')}} b
                    on
                              a.cal_year                   = b.cal_year
                              and a.cal_year_week_num_mon >= b.cal_year_week_num_mon
group by
          1
        , 2
        , 3
        , 4
        , 5
        , 6
        , 7
        , 8
        , 9
        , 10
        , 11