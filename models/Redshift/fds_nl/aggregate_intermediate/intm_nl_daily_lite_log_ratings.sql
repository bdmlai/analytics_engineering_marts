{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    *,
    'Y' AS commercial_overlap_ind
FROM
    {{ref('intm_nl_commercial_overlap_logs')}}
UNION ALL
SELECT
    *,
    'N' AS commercial_overlap_ind
FROM
    (
        SELECT
            *
        FROM
            {{ref('intm_nl_lite_log_ratings')}}
        MINUS
        SELECT
            *
        FROM
            {{ref('intm_nl_commercial_overlap_logs')}})