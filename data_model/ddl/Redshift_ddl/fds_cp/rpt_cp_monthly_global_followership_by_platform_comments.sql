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

COMMENT ON COLUMN fds_cp.rpt_cp_monthly_global_followership_by_platform.year IS 'Indicates the calendar year';
COMMENT ON COLUMN fds_cp.rpt_cp_monthly_global_followership_by_platform.month IS 'Indicates the calendar month';
COMMENT ON COLUMN fds_cp.rpt_cp_monthly_global_followership_by_platform.region_nm IS 'Represents region name';
COMMENT ON COLUMN fds_cp.rpt_cp_monthly_global_followership_by_platform.country_nm IS 'Represents the country name';
COMMENT ON COLUMN fds_cp.rpt_cp_monthly_global_followership_by_platform.account_name IS 'Indicates the account name';
COMMENT ON COLUMN fds_cp.rpt_cp_monthly_global_followership_by_platform.platform IS 'Indicates different platform like fb,ig,tw etc';
COMMENT ON COLUMN fds_cp.rpt_cp_monthly_global_followership_by_platform.followers IS 'Indicates followers count';
COMMENT ON COLUMN fds_cp.rpt_cp_monthly_global_followership_by_platform.etl_batch_id IS 'Unique ID of DBT Job used to insert the record';
COMMENT ON COLUMN fds_cp.rpt_cp_monthly_global_followership_by_platform.etl_insert_user_id IS 'Unique ID of the DBT user that was used to insert the record';
COMMENT ON COLUMN fds_cp.rpt_cp_monthly_global_followership_by_platform.etl_insert_rec_dttm IS 'Date Time information on when the DBT inserted the record';
COMMENT ON COLUMN fds_cp.rpt_cp_monthly_global_followership_by_platform.etl_update_user_id IS 'Unique ID of the DBT user which was used to update the record manually';
COMMENT ON COLUMN fds_cp.rpt_cp_monthly_global_followership_by_platform.etl_update_rec_dttm IS 'Date Time information on when the record was updated';    	
  	  
