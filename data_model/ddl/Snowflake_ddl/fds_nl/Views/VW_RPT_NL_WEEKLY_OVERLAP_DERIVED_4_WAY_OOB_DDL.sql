CREATE VIEW
    VW_RPT_NL_WEEKLY_OVERLAP_DERIVED_4_WAY_OOB
    (
        week_starting_date,
        input_type,
        coverage_area,
        market_break,
        demographic_group,
        playback_period_cd,
        program_combination,
        p2_total_unique_reach_proj,
        p2_total_unique_reach_percent,
        overlap_description,
        etl_batch_id,
        etl_insert_user_id,
        etl_insert_rec_dttm,
        etl_update_user_id,
        etl_update_rec_dttm
    ) AS
SELECT
    a.week_starting_date,
    a.input_type,
    a.coverage_area,
    a.market_break,
    a.demographic_group,
    a.playback_period_cd,
    a.program_combination,
    a.p2_total_unique_reach_proj,
    a.p2_total_unique_reach_percent,
    a.overlap_description,
    a.etl_batch_id,
    a.etl_insert_user_id,
    a.etl_insert_rec_dttm,
    a.etl_update_user_id,
    a.etl_update_rec_dttm
FROM
    rpt_nl_weekly_overlap_derived_4_way_oob a;