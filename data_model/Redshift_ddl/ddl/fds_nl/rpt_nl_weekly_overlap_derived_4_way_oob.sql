/*
************************************************************************************ 
    Date: 8/19/2020
    Version: 1.0
    Description: First Draft
    Contributor: Remya K Nair
    Adl' notes:  
    updated comments
**************************************************************************************/
/* date      | by      |    Details   */

CREATE TABLE
    fds_nl.rpt_nl_weekly_overlap_derived_4_way_oob
    (
        week_starting_date INTEGER ENCODE AZ64,
        input_type CHARACTER VARYING(23) ENCODE LZO,
        coverage_area CHARACTER VARYING(255) ENCODE LZO,
        market_break CHARACTER VARYING(255) ENCODE LZO,
        demographic_group CHARACTER VARYING(255) ENCODE LZO,
        playback_period_cd CHARACTER VARYING(255) ENCODE LZO,
        program_combination CHARACTER VARYING(255) ENCODE LZO,
        p2_total_unique_reach_proj BIGINT ENCODE AZ64,
        p2_total_unique_reach_percent CHARACTER VARYING(41) ENCODE LZO,
        overlap_description CHARACTER VARYING(79) ENCODE LZO,
        etl_batch_id CHARACTER VARYING(26) ENCODE LZO,
        etl_insert_user_id CHARACTER VARYING(15) ENCODE LZO,
        etl_insert_rec_dttm TIMESTAMP WITH TIME ZONE ENCODE AZ64,
        etl_update_user_id CHARACTER VARYING(1) ENCODE LZO,
        etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64
    );
  COMMENT ON TABLE fds_nl.rpt_nl_weekly_overlap_derived_4_way_oob
IS
    'Weekly schedule overlap report table';  
     COMMENT ON COLUMN fds_nl.rpt_nl_weekly_overlap_derived_4_way_oob.week_starting_date
          IS 'References dim_date_id primary key';
     COMMENT ON COLUMN fds_nl.rpt_nl_weekly_overlap_derived_4_way_oob.input_type
          IS 'Input Type is a customized column with value as staright Neilsen and Derived for schedules';
     COMMENT ON COLUMN fds_nl.rpt_nl_weekly_overlap_derived_4_way_oob.coverage_area
          IS 'NPOWER- Coverage Area';
     COMMENT ON COLUMN fds_nl.rpt_nl_weekly_overlap_derived_4_way_oob.market_break
          IS 'Identified Market break category by the broadcast nework';
     COMMENT ON COLUMN fds_nl.rpt_nl_weekly_overlap_derived_4_way_oob.demographic_group
          IS 'A comma separated list of demographic group';
     COMMENT ON COLUMN  fds_nl.rpt_nl_weekly_overlap_derived_4_way_oob.playback_period
         IS 'A comma separated list of data streams. Time-shifted viewing from DVR Playback or On-demand content with the same commercial load. Live (Live - Includes viewing that occurred during the live airing). Live+SD (Live + Same Day -Includes all playback that occurred within the same day of the liveairing). Live+3 (Live + 3 Days - Includes all playback that occurred within three days of the live airing). Live+7 (Live + 7 Days - Includes all playback that occurred within seven days of the live airing).';
     COMMENT ON COLUMN  fds_nl.rpt_nl_weekly_overlap_derived_4_way_oob.program_combination
          IS 'NPOWER - identifier for primary program/daypart selection as input within Nielsen report';
     COMMENT ON COLUMN  fds_nl.rpt_nl_weekly_overlap_derived_4_way_oob.p2_total_unique_reach_proj
          IS 'Unique Audience/impressions';
     COMMENT ON COLUMN  fds_nl.rpt_nl_weekly_overlap_derived_4_way_oob.P2_Total_Unique_Reach_Percent
          IS 'Custom Calculation based on  p2_total_unique_reach_proj';
     COMMENT ON COLUMN  fds_nl.rpt_nl_weekly_overlap_derived_4_way_oob.Overlap_Description
          IS 'Customized value for each Schedules both Straight Neilsen and Derived';
        