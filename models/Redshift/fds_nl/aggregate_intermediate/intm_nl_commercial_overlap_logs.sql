{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    *
FROM
    {{ref('intm_nl_lite_log_ratings')}}
WHERE
    (
        broadcast_date_id, src_playback_period_cd, src_demographic_group, mxm_source,
        program_telecast_rpt_starttime, program_telecast_rpt_endtime, min_of_pgm_value) IN
    (
        SELECT
            *
        FROM
            {{ref('intm_nl_commercial_overlap_minutes')}})
AND trim(LOWER(segmenttype)) <> 'commercial'