/*
*******************************************************************************************
Date : 10/27/2020
Version : 1.0
TableName : rpt_nplus_daily_milestone_complete_rates
Schema : fds_nplus
Contributor : Remya K Nair
Description : Reporting table consists of  Complete rates for PPV,live and tko streams
JIRA : PSTA-1045
***************************************************************************************************
Updates
TYPE JIRA DEVELOPER DATE DESCRIPTION
----- --------- ----- -----------
NEW PSTA-1045 Remya K Nair 10/27/2020 Initial Version  */

CREATE TABLE
    fds_nplus.rpt_nplus_daily_milestone_complete_rates 
    (
          type CHARACTER VARYING(1000) ENCODE LZO,
          external_id CHARACTER VARYING(256) ENCODE LZO,
          title CHARACTER VARYING(1000) ENCODE LZO,
	  premiere_date DATE ENCODE LZO,
	  complete_rate DOUBLE PRECISION,
          viewers_count BIGINT ENCODE LZO ,
          etl_batch_id CHARACTER VARYING(26) ENCODE LZO,
          etl_insert_user_id CHARACTER VARYING(15) ENCODE LZO,
          etl_insert_rec_dttm TIMESTAMP WITH TIME ZONE ENCODE AZ64,
          etl_update_user_id CHARACTER VARYING(1) ENCODE LZO,
          etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64
    );
