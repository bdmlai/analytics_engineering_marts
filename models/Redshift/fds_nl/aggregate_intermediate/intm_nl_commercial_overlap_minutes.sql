{{
  config({
		"materialized": 'ephemeral'
  })
}}
(
 SELECT DISTINCT
     broadcast_date_id,
     src_playback_period_cd,
     src_demographic_group,
     mxm_source,
     program_telecast_rpt_starttime,
     program_telecast_rpt_endtime,
     min_of_pgm_value
 FROM
     (
         SELECT
             broadcast_date_id,
             src_playback_period_cd,
             src_demographic_group,
             mxm_source,
             program_telecast_rpt_starttime,
             program_telecast_rpt_endtime,
             min_of_pgm_value,
             logentryguid
         FROM
             {{ref('intm_nl_lite_log_ratings')}}
         WHERE
             trim(LOWER(segmenttype)) = 'commercial'))
INTERSECT
    (
        SELECT DISTINCT
            broadcast_date_id,
            src_playback_period_cd,
            src_demographic_group,
            mxm_source,
            program_telecast_rpt_starttime,
            program_telecast_rpt_endtime,
            min_of_pgm_value
        FROM
            (
                SELECT
                    broadcast_date_id,
                    src_playback_period_cd,
                    src_demographic_group,
                    mxm_source,
                    program_telecast_rpt_starttime,
                    program_telecast_rpt_endtime,
                    min_of_pgm_value,
                    logentryguid
                FROM
                    {{ref('intm_nl_lite_log_ratings')}}
                WHERE
                    trim(LOWER(segmenttype)) <> 'commercial'))