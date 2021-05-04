{{ config({ "schema": 'fds_le', "materialized": 'ephemeral',"tags": 'rpt_le_weekly_consolidated_kpi' }) }}
select
       a.granularity
     , a.platform
     , a.platform as type
     , a.metric
     , a.cal_year                     as year
     , a.cal_mth_num                  as month
     , a.cal_year_week_num_mon        as week
     , a.cal_year_mon_week_begin_date as start_date
     , a.cal_year_mon_week_end_date   as end_date
     , a.value
     , a.prev_cal_year                     as prev_year
     , a.prev_cal_year_week_num_mon        as prev_year_week
     , a.prev_cal_year_mon_week_begin_date as prev_year_start_date
     , a.prev_cal_year_mon_week_end_date   as prev_year_end_date
     , a.prev_year_value
from
       (
              select *
              from
                     {{ref('intm_le_pivot_wkly')}}
              union all
              select *
              from
                     {{ref('intm_le_pivot_mthly')}}
              union all
              select *
              from
                     {{ref('intm_le_pivot_yrly')}}
       )
       a