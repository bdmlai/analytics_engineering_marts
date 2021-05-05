{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    *
FROM
    (
        SELECT
            *,
            rank() over (partition BY state ORDER BY state_regulation_update_date_max DESC) AS
            regulation_change_rank
        FROM
            (
                SELECT
                    state,
                    face_covering_requirement_bucket_group,
                    MAX(state_regulation_update_date) AS state_regulation_update_date_max
                FROM
                    {{ref('intm_le_state_regulation_change')}}
                WHERE
                    state IN
                    (
                        SELECT
                            state
                        FROM
                            (
                                SELECT
                                    *
                                FROM
                                    {{ref('intm_le_state_regulation_change')}}
                                WHERE
                                    state_regulation_update_date =
                                    (
                                        SELECT
                                            MAX(state_regulation_update_date)
                                        FROM
                                            {{ref('intm_le_state_regulation_change')}})) a
                        WHERE
                            EXISTS
                            (
                                SELECT
                                    face_covering_requirement_bucket_group
                                FROM
                                    {{ref('intm_le_state_regulation_change')}} b
                                WHERE
                                    a.state = b.state
                                AND trim(REPLACE(LOWER(a.face_covering_requirement_bucket_group),
                                    ' ','')) <> trim(REPLACE(LOWER
                                    (b.face_covering_requirement_bucket_group),' ',''))))
                GROUP BY
                    1,2
                HAVING
                    state_regulation_update_date_max <
                    (
                        SELECT
                            MAX(state_regulation_update_date)
                        FROM
                            {{ref('intm_le_state_regulation_change')}})))
WHERE
    regulation_change_rank = 1