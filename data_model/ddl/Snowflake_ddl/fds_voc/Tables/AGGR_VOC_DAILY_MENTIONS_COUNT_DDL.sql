/*
******************VERSION MODIFICATION*****************************************************************************************
*    TABLE NAME	:	AGGR_VOC_DAILY_MENTIONS_COUNT
*    SCHEMA	:       FDS_VOC
*    DEVELOPER	:   RAHUL CHANDRAN
*    DATE	:       14-JAN-2021
*    DESCRIPTION:	Daily Aggregate VOC Mentions Count Table provides count of tweets about mentions on daily-basis
*******************************************************************************************************************************
*/
/* Date       | By     				 | Details                                                                      */
/* 01/14/2021 | RAHUL CHANDRAN       | Development of Daily Aggregate VOC Mentions Count Table , JIRA PTM-435       */
/*
*******************************************************************************************************************************
*/
CREATE TABLE AGGR_VOC_DAILY_MENTIONS_COUNT (
	POSTED_DATE DATE,
	MENTIONS VARCHAR(255),
	COUNT_TWEETS NUMBER(38,0),
	ETL_BATCH_ID VARCHAR(50),
	ETL_INSERT_USER_ID VARCHAR(25),
	ETL_INSERT_REC_DTTM TIMESTAMP_NTZ(9),
	ETL_UPDATE_USER_ID VARCHAR(25),
	ETL_UPDATE_REC_DTTM TIMESTAMP_NTZ(9)
);