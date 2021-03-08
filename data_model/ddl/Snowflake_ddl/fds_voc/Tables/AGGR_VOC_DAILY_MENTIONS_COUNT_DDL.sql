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
	POSTED_DATE DATE COMMENT 'The date on when the tweet is posted',
	MENTIONS VARCHAR(255) COMMENT 'It gives detail which tweet is mentioning about',
	COUNT_TWEETS NUMBER(38,0) COMMENT 'Gives the count of total number of tweets',
	ETL_BATCH_ID VARCHAR(50) COMMENT 'Unique ID of DBT Job used to insert the record',
	ETL_INSERT_USER_ID VARCHAR(25) COMMENT 'Unique ID of the DBT user that was used to insert the record',
	ETL_INSERT_REC_DTTM TIMESTAMP_NTZ(9) COMMENT 'Date Time information on when the DBT inserted the record',
	ETL_UPDATE_USER_ID VARCHAR(25) COMMENT 'Unique ID of the DBT user which was used to update the record manually',
	ETL_UPDATE_REC_DTTM TIMESTAMP_NTZ(9) COMMENT 'Date Time information on when the record was updated'
);