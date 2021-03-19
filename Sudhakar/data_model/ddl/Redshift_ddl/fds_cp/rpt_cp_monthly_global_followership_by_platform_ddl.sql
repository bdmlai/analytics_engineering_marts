/*
*******************************************************************************************
Date : 11/05/2020
Version : 1.0
TableName : rpt_cp_monthly_global_followership_by_platform
Schema : fds_cp
Contributor : Hima Dasan
Description : Reporting table consists of  consists of monthly followership count for platsforms - facebook,instagram,twitter,youtube and china social on country and account level
JIRA : PSTA-1161
***************************************************************************************************
Updates
TYPE JIRA DEVELOPER DATE DESCRIPTION
----- --------- ----- -----------
NEW PSTA-1161 Hima Dasan 11/05/2020 Initial Version  */

CREATE TABLE
    fds_cp.rpt_cp_monthly_global_followership_by_platform 
    (
        YEAR SMALLINT ENCODE AZ64,
        MONTH INTEGER ENCODE AZ64,
          region_nm CHARACTER VARYING(1000) ENCODE LZO,
		  country_nm CHARACTER VARYING(1000) ENCODE LZO,
	  account_name CHARACTER VARYING(256) ENCODE LZO,
	  platform CHARACTER VARYING(256) ENCODE LZO,
          followers BIGINT ENCODE LZO ,
          etl_batch_id CHARACTER VARYING(26) ENCODE LZO,
          etl_insert_user_id CHARACTER VARYING(15) ENCODE LZO,
          etl_insert_rec_dttm TIMESTAMP WITH TIME ZONE ENCODE AZ64,
          etl_update_user_id CHARACTER VARYING(1) ENCODE LZO,
          etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64
    );
