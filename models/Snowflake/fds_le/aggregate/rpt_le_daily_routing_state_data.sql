{{
  config({
		"schema": 'fds_le',
		"materialized": 'table','tags': 'Phase 5A',"persist_docs": {'relation' : true, 'columns' : true},
		"post-hook": 'grant select on {{this}} to public'
  })
}}
SELECT
    a.*,
    c.state_covid_growth_rank,
    COALESCE(b.large_gatherings_ban_bucket_group, a.large_gatherings_ban_bucket_group) AS
                                                large_gatherings_ban_previous,
    (b.state_regulation_update_date_max + 1)                        AS large_gatherings_change_date,
    COALESCE(d.face_covering_requirement_bucket_group, a.face_covering_requirement_bucket_group) AS
                                                face_covering_requirement_previous,
    (d.state_regulation_update_date_max + 1) AS face_covering_requirement_change_date,
    ('DBT_' || TO_CHAR(((convert_timezone ('UTC', GETDATE()))::TIMESTAMP),'YYYY_MM_DD_HH24_MI_SS')
    || '_5A')::VARCHAR(255)                            AS etl_batch_id,
    'SVC_PROD_BI_DBT_USER'                             AS etl_insert_user_id,
    ((convert_timezone ('UTC', GETDATE()))::TIMESTAMP) AS etl_insert_rec_dttm,
    NULL::VARCHAR(255)                                 AS etl_update_user_id,
    CAST(NULL AS TIMESTAMP)                            AS etl_update_rec_dttm
FROM
    {{ref('intm_le_daily_routing_state_data')}} a
LEFT JOIN
    {{ref('intm_le_regulation_change_gathering')}} b
ON
    trim(LOWER(a.state)) = trim(LOWER(b.state))
LEFT JOIN
    {{ref('intm_le_routing_state_covid_growth')}} c
ON
    trim(LOWER(a.state)) = trim(LOWER(c.state))
LEFT JOIN
    {{ref('intm_le_regulation_change_facecovering')}} d
ON
    trim(LOWER(a.state)) = trim(LOWER(d.state))