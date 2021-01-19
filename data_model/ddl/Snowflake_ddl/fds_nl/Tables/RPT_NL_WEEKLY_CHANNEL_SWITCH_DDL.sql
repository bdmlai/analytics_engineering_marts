CREATE TABLE RPT_NL_WEEKLY_CHANNEL_SWITCH (
	BROADCAST_DATE TIMESTAMP_NTZ(9) COMMENT 'Broadcast date for the given channel',
	COVERAGE_AREA VARCHAR(255) COMMENT 'NPOWER- Coverage Area',
	SRC_MARKET_BREAK VARCHAR(255) COMMENT 'Identified Marcket break category by the broadcast nework',
	SRC_BROADCAST_NETWORK_NAME VARCHAR(255) COMMENT 'Viewing source of the network channel',
	SRC_DEMOGRAPHIC_GROUP VARCHAR(255) COMMENT 'A comma separated list of demographic groups (e.g. Females 18 to 49 and Males 18 - 24 input as F18-49,M18-24).',
	TIME_MINUTE VARCHAR(10) COMMENT 'minute of measurement the intervel of switching beahiour is measured',
	MC_US_AA000 NUMBER(38,0) COMMENT 'Metrics for most current US audience average projection in thousants',
	ABSOLUTE_SET_OFF_OFF_AIR NUMBER(35,6),
	ABSOLUTE_STAY NUMBER(35,6) COMMENT 'unique viewers who stayed in the same network',
	STAY_PERCENT NUMBER(38,6) COMMENT 'percentage value of viewers stayed in same network',
	ABSOLUTE_SWITCH NUMBER(38,6) COMMENT 'unique viewers who swicthed to different network',
	SWITCH_PERCENT NUMBER(38,6) COMMENT 'percentage value of unique viewers who swicthed to different network',
	SWITCH_PERCENT_RANK NUMBER(38,0) COMMENT 'ranking based on percentage switch',
	ETL_BATCH_ID VARCHAR(26),
	ETL_INSERT_USER_ID VARCHAR(15),
	ETL_INSERT_REC_DTTM TIMESTAMP_TZ(9),
	ETL_UPDATE_USER_ID VARCHAR(1),
	ETL_UPDATE_REC_DTTM TIMESTAMP_NTZ(9)
)COMMENT='## Implementation Detail\n* Date        : 06/19/2020\n* Version     : 1.0\n* TableName   : rpt_nl_weekly_channel_switch\n* Schema   : fds_nl\n* Contributor : Hima Dasan\n* Description : rpt_nl_weekly_channel_switch view consists of absolute stay ,absolute switch and ranking based on switch for WWE and AEW Programs \n\n## Schedule Details\n* Frequency : Daily ; 12:00 A.M EST (Sun-Mon)\n* Dependent Jobs (process_name ; process_id) : t_di_nielsen_fact_nl_weekly_live_switching_behavior_destination_dist_abac ; 12128,  t_di_nielsen_fact_nl_minxmin_ratings_aew_abac ; 12133, t_di_nielsen_fact_nl_minxmin_ratings_nxt_abac ; 12135, t_di_nielsen_fact_nl_minxmin_ratings_raw_abac ; 12136 and t_di_nielsen_fact_nl_minxmin_ratings_smackdown_abac ; 12137 (Sun-Mon)\n\n## Maintenance Log\n* Date : 06/19/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of Phase 4b Project.\n* Date : 08/28/2020 ; Developer: Hima Dasan ; Change: Enhancement to remove commercial break minutes and starting and ending 5 minutes from ranking.'
;