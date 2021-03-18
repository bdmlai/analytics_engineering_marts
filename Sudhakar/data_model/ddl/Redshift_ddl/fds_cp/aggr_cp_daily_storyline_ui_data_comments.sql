/*
*******************************************************************************************
Date : 01/29/2021
Version : 1.0
TableName : aggr_cp_daily_storyline_ui_data
Schema : fds_cp
Contributor : Hima Dasan
Description : Aggregation based on story,show and date level.This data used to issue weekly storyline report after shows and support adhoc/forcasting analysis on storyline level .
JIRA : PSTA-2158
***************************************************************************************************
Updates
TYPE JIRA DEVELOPER DATE DESCRIPTION
----- --------- ----- -----------
NEW PSTA-1897 Hima Dasan 01/29/2021 Initial Version */


COMMENT ON COLUMN fds_cp.aggr_cp_daily_storyline_ui_data.show_date IS 'The date on when the show aired';
COMMENT ON COLUMN fds_cp.aggr_cp_daily_storyline_ui_data.show_name IS 'Represents the show name like Raw,Smackdown etc';
COMMENT ON COLUMN fds_cp.aggr_cp_daily_storyline_ui_data.story IS 'Indicates the storyline';
COMMENT ON COLUMN fds_cp.aggr_cp_daily_storyline_ui_data.talent IS 'Indicates the talent involved in the storyline';
COMMENT ON COLUMN fds_cp.aggr_cp_daily_storyline_ui_data.comments IS 'Indicates the comments for the storyline';
COMMENT ON COLUMN fds_cp.aggr_cp_daily_storyline_ui_data.appeared IS 'Indicates the inpoint and outpoint est time related to the storyline';
COMMENT ON COLUMN fds_cp.aggr_cp_daily_storyline_ui_data.average_viewers_p18_49 IS 'Indicates  average viewers for  src_demographic_group in Persons 18 - 49';
COMMENT ON COLUMN fds_cp.aggr_cp_daily_storyline_ui_data.avg_p2_plus_viewership IS 'Indicates average viewers for p2-plus demographic group';
COMMENT ON COLUMN fds_cp.aggr_cp_daily_storyline_ui_data.agg_nielsen_tw_interactions IS 'Indicates average nielsen twitter interactions';
COMMENT ON COLUMN fds_cp.aggr_cp_daily_storyline_ui_data.tw_interactions_Ranking IS 'Indicates Twitter interactions ranking based on each storyline';
COMMENT ON COLUMN fds_cp.aggr_cp_daily_storyline_ui_data.etl_batch_id IS 'Unique ID of DBT Job used to insert the record';
COMMENT ON COLUMN fds_cp.aggr_cp_daily_storyline_ui_data.etl_insert_user_id IS 'Unique ID of the DBT user that was used to insert the record';
COMMENT ON COLUMN fds_cp.aggr_cp_daily_storyline_ui_data.etl_insert_rec_dttm IS 'Date Time information on when the DBT inserted the record';
COMMENT ON COLUMN fds_cp.aggr_cp_daily_storyline_ui_data.etl_update_user_id IS 'Unique ID of the DBT user which was used to update the record manually';
COMMENT ON COLUMN fds_cp.aggr_cp_daily_storyline_ui_data.etl_update_rec_dttm IS 'Date Time information on when the record was updated';    	







