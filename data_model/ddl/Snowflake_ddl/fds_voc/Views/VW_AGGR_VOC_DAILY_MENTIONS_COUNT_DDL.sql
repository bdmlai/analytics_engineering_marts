/*
******************VERSION MODIFICATION*****************************************************************************************
*    VIEW NAME	:	AGGR_VOC_DAILY_MENTIONS_COUNT
*    SCHEMA	:       FDS_VOC
*    DEVELOPER	:   RAHUL CHANDRAN
*    DATE	:       14-JAN-2021
*    DESCRIPTION:	Daily Aggregate VOC Mentions Count view provides count of tweets about mentions referencing from Daily 
*                   Aggregate VOC Mentions Count Table
*******************************************************************************************************************************
*/
/* Date       | By     				 | Details                                                                      */
/* 01/14/2021 | RAHUL CHANDRAN       | Development of Daily Aggregate VOC Mentions Count View , JIRA PTM-435        */
/*
*******************************************************************************************************************************
*/
CREATE VIEW VW_AGGR_VOC_DAILY_MENTIONS_COUNT
AS
SELECT * FROM AGGR_VOC_DAILY_MENTIONS_COUNT;