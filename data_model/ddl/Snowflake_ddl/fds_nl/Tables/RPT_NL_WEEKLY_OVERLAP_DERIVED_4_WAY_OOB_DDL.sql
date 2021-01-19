
	CREATE TABLE RPT_NL_WEEKLY_OVERLAP_DERIVED_4_WAY_OOB (
	WEEK_STARTING_DATE NUMBER(38,0) COMMENT 'References dim_date_id primary key',
	INPUT_TYPE VARCHAR(23) COMMENT 'Input Type is a customized column with value as staright Neilsen and Derived for schedules',
	COVERAGE_AREA VARCHAR(255) COMMENT 'NPOWER- Coverage Area',
	MARKET_BREAK VARCHAR(255) COMMENT 'Identified Marcket break category by the broadcast nework',
	DEMOGRAPHIC_GROUP VARCHAR(255) COMMENT 'A comma separated list of demographic groups (e.g. Females 18 to 49 and Males 18 - 24 input as F18-49,M18-24).',
	PLAYBACK_PERIOD_CD VARCHAR(255) COMMENT 'A comma separated list of data streams. Time-shifted viewing from DVR Playback or On-demand content with the same commercial load.• Live (Live - Includes viewing that occurred during the live airing).• Live+SD (Live + Same Day -Includes all playback that occurred within the same day of the liveairing).• Live+3 (Live + 3 Days - Includes all playback that occurred within three days of the live airing).• Live+7 (Live + 7 Days - Includes all playback that occurred within seven days of the live airing).',
	PROGRAM_COMBINATION VARCHAR(255) COMMENT 'NPOWER - identifier for primary program/daypart selection as input within Nielsen report' ,
	P2_TOTAL_UNIQUE_REACH_PROJ NUMBER(38,0) COMMENT 'Unique Audience/impressions',
	P2_TOTAL_UNIQUE_REACH_PERCENT VARCHAR(41) COMMENT 'Custom Calculation based on  AA_Reac_Proj_000',
	OVERLAP_DESCRIPTION VARCHAR(79) COMMENT 'Customized value for each Schedules both Straight Neilsen and Derived',
	ETL_BATCH_ID VARCHAR(26) COMMENT 'Unique ID of DBT Job used to insert the record',
	ETL_INSERT_USER_ID VARCHAR(15) COMMENT 'Unique ID of the DBT user that was used to insert the record',
	ETL_INSERT_REC_DTTM TIMESTAMP_TZ(9) COMMENT 'Date Time information on when the DBT inserted the record',
	ETL_UPDATE_USER_ID VARCHAR(1) COMMENT 'Unique ID of the DBT user which was used to update the record manually',
	ETL_UPDATE_REC_DTTM TIMESTAMP_NTZ(9)  COMMENT 'Date Time information on when the record was updated'
)COMMENT='## Implementation Detail\n*   Date        : 06/19/2020\n  Version     : 1.0\n  TableName   : rpt_nl_weekly_overlap_derived_4_way_oob\n   Schema	   : fds_nl\n   Contributor : Remya K Nair\n   Description : rpt_nl_weekly_overlap_derived_4_way_oob view consists of weekly  overlap program schedules and unique reach for each time period.' 
