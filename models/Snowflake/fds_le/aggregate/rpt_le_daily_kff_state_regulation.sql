{{
  config({
		"schema": 'fds_le',
		"materialized": 'incremental','tags': 'Phase 5A',
		"unique_key": 'concat(state_regulation_update_date, state)',
		"incremental_strategy": 'delete+insert',
		"persist_docs": {'relation' : true, 'columns' : true},
		"post-hook": 'grant select on {{this}} to public'
  })
}}
SELECT
    a.state_regulation_update_date,
    a.state,
    a.status_of_reopening::VARCHAR(255)                AS status_of_reopening,
    a.stay_at_home_order::VARCHAR(255)                 AS stay_at_home_order,
    a.mandatory_quarantine_for_travelers::VARCHAR(255) AS mandatory_quarantine_for_travelers,
    a.non_essential_business_closures::VARCHAR(255)    AS non_essential_business_closures,
    a.large_gatherings_ban::VARCHAR(255)               AS large_gatherings_ban,
    CASE
        WHEN b.lkp_description IS NULL
        THEN trim(a.large_gatherings_ban)::VARCHAR(500)
        ELSE trim(b.lkp_description)
    END                               AS large_gatherings_ban_bucket_group,
    a.restaurant_limits::VARCHAR(255)         AS restaurant_limits,
    a.bar_closures::VARCHAR(255)              AS bar_closures,
    a.face_covering_requirement::VARCHAR(255) AS face_covering_requirement,
    CASE
        WHEN c.lkp_description IS NULL
        THEN trim(a.face_covering_requirement)::VARCHAR(500)
        ELSE trim(c.lkp_description)
    END                                           AS face_covering_requirement_bucket_group,
    a.primary_election_postponement::VARCHAR(255) AS primary_election_postponement,
    a.emergency_declaration::VARCHAR(255)         AS emergency_declaration,
    ('DBT_' || TO_CHAR(((convert_timezone ('UTC', GETDATE()))::TIMESTAMP),'YYYY_MM_DD_HH24_MI_SS')
    || '_5A')::VARCHAR(255)                            AS etl_batch_id,
    'SVC_PROD_BI_DBT_USER'                             AS etl_insert_user_id,
    ((convert_timezone ('UTC', GETDATE()))::TIMESTAMP) AS etl_insert_rec_dttm,
    NULL::VARCHAR(255)                                 AS etl_update_user_id,
    CAST(NULL AS TIMESTAMP)                            AS etl_update_rec_dttm
FROM
    (
        SELECT
            *
        FROM
            (
                SELECT
                    *,
                    row_number()over(partition BY state, state_regulation_update_date ORDER BY
                    status_of_reopening DESC) AS rank1
                FROM
                    {{ref('intm_le_kff_state_regulation')}})
        WHERE
            rank1 = 1) a
LEFT JOIN
    (
        SELECT
            lkp_code,
            lkp_description
        FROM
            {{source('prod_entdwdb.cdm','lookup_table')}}
        WHERE
            lkp_name = 'Regulations_LG_Buckets') b
ON
    trim(LOWER(a.large_gatherings_ban)) = trim(LOWER(b.lkp_code))
LEFT JOIN
    (
        SELECT
            lkp_code,
            lkp_description
        FROM
            {{source('prod_entdwdb.cdm','lookup_table')}}
        WHERE
            lkp_name = 'Regulations_FC_Buckets') c
ON
    trim(LOWER(a.face_covering_requirement)) = trim(LOWER(c.lkp_code))