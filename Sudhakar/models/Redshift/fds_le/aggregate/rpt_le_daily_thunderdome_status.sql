{{
  config({
		'schema': 'fds_le',"materialized": 'table','tags': "Phase 5A","persist_docs": {'relation' : true, 'columns' : true}
  })
}}
select distinct
    a.*,
    case
        when b.uid_hit is null
        then 'false'
        else 'true'
    end                                                          as attempted_to_attend_flag,
    ('DBT_' || to_char(sysdate, 'YYYY_MM_DD_HH_MI_SS') || '_5A') as etl_batch_id,
    'bi_dbt_user_prd'                                            as etl_insert_user_id,
    sysdate                                                      as etl_insert_rec_dttm,
    null                                                         as etl_update_user_id,
    cast(null as timestamp)                                      as etl_update_rec_dttm
from
    {{ref('intm_le_fan_status_rpt')}} a
left join
    {{ref('intm_le_daily_hits')}} b
on
    a.event_date = b.date
and a.user_id = b.uid_hit