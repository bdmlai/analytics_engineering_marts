{{
  config({
		'schema': 'fds_pii',
		"materialized": 'table','tags': "Phase 5A","persist_docs": {'relation' : true, 'columns' : true}
  })
}}
select distinct
    a.event_id,
	a.event_name,
	a.event_date,
	a.reg_date,
	a.user_id,
	a.email,
	a.country,
	a.region,
	a.virtual_seat_attended,
	a.as_on_date,
	a.etl_batch_id_source,
	a.fan_status,
    case
        when b.uid_hit is null
        then 'false'
        else 'true'
    end                                                          as attempted_to_attend_flag,
	b.full_visitor_id,
	b.visit_id,
	b.total_time_on_site,
	b.trafficsource_campaign,
	b.device_devicecategory,
	b.geonetwork_country,
	b.as_on_date_ga,
    ('DBT_' || to_char(sysdate, 'YYYY_MM_DD_HH_MI_SS') || '_5A') as etl_batch_id,
    'bi_dbt_user_prd'                                            as etl_insert_user_id,
    sysdate                                                      as etl_insert_rec_dttm,
    null                                                         as etl_update_user_id,
    cast(null as timestamp)                                      as etl_update_rec_dttm
from
    {{ref('intm_pii_fan_status_rpt')}} a
left join
    {{ref('intm_pii_daily_sessions_detail')}} b
on
    a.event_date = b.date
and a.user_id    = b.uid_hit