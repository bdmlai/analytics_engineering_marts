{{ config({ "schema": 'fds_le', "materialized": 'ephemeral',"tags": 'rpt_le_weekly_consolidated_kpi' }) }}
select
          b.*
        , a.no_of_total_events_wk::decimal(15,1)
        , a.no_of_house_events_wk::decimal(15,1)
        , a.no_of_raw_house_events_wk::decimal(15,1)
        , a.no_of_smackdown_house_events_wk::decimal(15,1)
        , a.no_of_combined_house_events_wk::decimal(15,1)
        , a.no_of_tv_events_wk::decimal(15,1)
        , a.no_of_raw_tv_events_wk::decimal(15,1)
        , a.no_of_smackdown_tv_events_wk::decimal(15,1)
        , a.no_of_combined_tv_events_wk::decimal(15,1)
        , a.no_of_ppv_events_wk::decimal(15,1)
        , a.total_paid_attendance_wk::decimal(15,1)
        , a.capacity_wk::decimal(15,1)
        , a.total_paid_utilization_wk::decimal(15,3)
        , a.avg_total_attendance_wk::decimal(15,1)
        , a.avg_house_event_attendance_wk::decimal(15,1)
        , a.avg_raw_house_event_attendance_wk::decimal(15,1)
        , a.avg_smackdown_house_event_attendance_wk::decimal(15,1)
        , a.avg_cmb_house_event_attendance_wk::decimal(15,1)
        , a.avg_tv_event_attendance_wk::decimal(15,1)
        , a.avg_raw_tv_event_attendance_wk::decimal(15,1)
        , a.avg_smackdown_tv_event_attendance_wk::decimal(15,1)
        , a.avg_cmb_tv_event_attendance_wk::decimal(15,1)
        , a.avg_ppv_event_attendance_wk::decimal(15,1)
        , 'Live Events'::varchar(12) as platform
from
          {{ref('intm_le_dim_dates')}} b
          left join
                    (
                              select
                                        date_trunc('week',a.event_date) as monday_date
                                      , count(a.dim_event_id)           as no_of_total_events_wk
                                      , count
                                                  (
                                                            case
                                                                      when a.event_type_cd = 'LE'
                                                                                then a.dim_event_id
                                                                                else null
                                                            end
                                                  )
                                        as no_of_house_events_wk
                                      , count
                                                  (
                                                            case
                                                                      when a.event_type_cd     = 'LE'
                                                                                and a.brand_nm = 'RAW'
                                                                                then a.dim_event_id
                                                                                else null
                                                            end
                                                  )
                                        as no_of_raw_house_events_wk
                                      , count
                                                  (
                                                            case
                                                                      when a.event_type_cd     = 'LE'
                                                                                and a.brand_nm = 'SMD'
                                                                                then a.dim_event_id
                                                                                else null
                                                            end
                                                  )
                                        as no_of_smackdown_house_events_wk
                                      , count
                                                  (
                                                            case
                                                                      when a.event_type_cd     = 'LE'
                                                                                and a.brand_nm = 'CMB'
                                                                                then a.dim_event_id
                                                                                else null
                                                            end
                                                  )
                                        as no_of_combined_house_events_wk
                                      , count
                                                  (
                                                            case
                                                                      when a.event_type_cd = 'TV'
                                                                                then a.dim_event_id
                                                                                else null
                                                            end
                                                  )
                                        as no_of_tv_events_wk
                                      , count
                                                  (
                                                            case
                                                                      when a.event_type_cd     = 'TV'
                                                                                and a.brand_nm = 'RAW'
                                                                                then a.dim_event_id
                                                                                else null
                                                            end
                                                  )
                                        as no_of_raw_tv_events_wk
                                      , count
                                                  (
                                                            case
                                                                      when a.event_type_cd     = 'TV'
                                                                                and a.brand_nm = 'SMD'
                                                                                then a.dim_event_id
                                                                                else null
                                                            end
                                                  )
                                        as no_of_smackdown_tv_events_wk
                                      , count
                                                  (
                                                            case
                                                                      when a.event_type_cd     = 'TV'
                                                                                and a.brand_nm = 'CMB'
                                                                                then a.dim_event_id
                                                                                else null
                                                            end
                                                  )
                                        as no_of_combined_tv_events_wk
                                      , count
                                                  (
                                                            case
                                                                      when a.event_type_cd = 'PPV'
                                                                                then a.dim_event_id
                                                                                else null
                                                            end
                                                  )
                                                                                                                                as no_of_ppv_events_wk
                                      , sum(a.les_total_paid)                                                                   as total_paid_attendance_wk
                                      , sum(a.les_event_capacity)                                                               as capacity_wk
                                      , sum(a.les_total_paid)::decimal(15,3)/NULLIF(sum(a.les_event_capacity)::decimal(15,3),0) as total_paid_utilization_wk
                                      , avg(a.les_total_paid)                                                                   as avg_total_attendance_wk
                                      , avg
                                                  (
                                                            case
                                                                      when a.event_type_cd = 'LE'
                                                                                then a.les_total_paid
                                                                                else null
                                                            end
                                                  )
                                        as avg_house_event_attendance_wk
                                      , avg
                                                  (
                                                            case
                                                                      when a.event_type_cd     = 'LE'
                                                                                and a.brand_nm = 'RAW'
                                                                                then a.les_total_paid
                                                                                else null
                                                            end
                                                  )
                                        as avg_raw_house_event_attendance_wk
                                      , avg
                                                  (
                                                            case
                                                                      when a.event_type_cd     = 'LE'
                                                                                and a.brand_nm = 'SMD'
                                                                                then a.les_total_paid
                                                                                else null
                                                            end
                                                  )
                                        as avg_smackdown_house_event_attendance_wk
                                      , avg
                                                  (
                                                            case
                                                                      when a.event_type_cd     = 'LE'
                                                                                and a.brand_nm = 'CMB'
                                                                                then a.les_total_paid
                                                                                else null
                                                            end
                                                  )
                                        as avg_cmb_house_event_attendance_wk
                                      , avg
                                                  (
                                                            case
                                                                      when a.event_type_cd = 'TV'
                                                                                then a.les_total_paid
                                                                                else null
                                                            end
                                                  )
                                        as avg_tv_event_attendance_wk
                                      , avg
                                                  (
                                                            case
                                                                      when a.event_type_cd     = 'TV'
                                                                                and a.brand_nm = 'RAW'
                                                                                then a.les_total_paid
                                                                                else null
                                                            end
                                                  )
                                        as avg_raw_tv_event_attendance_wk
                                      , avg
                                                  (
                                                            case
                                                                      when a.event_type_cd     = 'TV'
                                                                                and a.brand_nm = 'SMD'
                                                                                then a.les_total_paid
                                                                                else null
                                                            end
                                                  )
                                        as avg_smackdown_tv_event_attendance_wk
                                      , avg
                                                  (
                                                            case
                                                                      when a.event_type_cd     = 'TV'
                                                                                and a.brand_nm = 'CMB'
                                                                                then a.les_total_paid
                                                                                else null
                                                            end
                                                  )
                                        as avg_cmb_tv_event_attendance_wk
                                      , avg
                                                  (
                                                            case
                                                                      when a.event_type_cd = 'PPV'
                                                                                then a.les_total_paid
                                                                                else null
                                                            end
                                                  )
                                        as avg_ppv_event_attendance_wk
                              from
                                        {{source('fds_le','fact_combined_ticket_sale')}} a
                                        left join
                                                  {{source('cdm','dim_event')}} b
                                                  on
                                                            a.dim_event_id = b.dim_event_id
                              where
                                        a.event_date  >= trunc(dateadd('year',-1,date_trunc('year',getdate())))
                                        and a.brand_nm!='NXT'
                                        and a.country_nm in ('united states'
                                                           ,'canada')
                                        and b.event_status != 'Cancelled'
                              group by
                                        1
                    )
                    a
                    on
                              trunc(a.monday_date) = b.cal_year_mon_week_begin_date