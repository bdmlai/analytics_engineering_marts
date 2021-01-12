/*
*******************************************************************************************
Date : 12/07/2020
Version : 1.0
TableName : rpt_cpg_monthly_royalty
Schema : fds_cpg
Contributor : Hima Dasan
Description : Reporting table consists of monthly royalty finance details . Has item shipped,returned and refunded details of items in monthly basis.
JIRA : PBCS-1457
***************************************************************************************************
Updates
TYPE JIRA DEVELOPER DATE DESCRIPTION
----- --------- ----- -----------
NEW PBCS-1457 Hima Dasan 12/07/2020 Initial Version  */


COMMENT ON COLUMN fds_cpg.rpt_cpg_monthly_royalty.source IS 'Indicates the sources like radial,clientbase etc';
COMMENT ON COLUMN fds_cpg.rpt_cpg_monthly_royalty.currency_type IS 'Indicates currency type';
COMMENT ON COLUMN fds_cpg.rpt_cpg_monthly_royalty.report_month IS 'Reporting month';
COMMENT ON COLUMN fds_cpg.rpt_cpg_monthly_royalty.month IS 'Indicates month name';
COMMENT ON COLUMN fds_cpg.rpt_cpg_monthly_royalty.year IS 'Indicates year';
COMMENT ON COLUMN fds_cpg.rpt_cpg_monthly_royalty.quarter IS 'indicates Quarter number';
COMMENT ON COLUMN fds_cpg.rpt_cpg_monthly_royalty.item_id IS 'item id for item from reference system (JDE)';
COMMENT ON COLUMN fds_cpg.rpt_cpg_monthly_royalty.item_description IS 'Description of the item from reference system (JDE)';
COMMENT ON COLUMN fds_cpg.rpt_cpg_monthly_royalty.royalty_code IS 'Name of royalty for item from reference system (JDE). Eg. WWF011051, WWE999999';
COMMENT ON COLUMN fds_cpg.rpt_cpg_monthly_royalty.shipped_quantity IS 'Indicates shipped quantity';
COMMENT ON COLUMN fds_cpg.rpt_cpg_monthly_royalty.shipped_sales IS 'Indicates shipped sales';
COMMENT ON COLUMN fds_cpg.rpt_cpg_monthly_royalty.returned_quantity IS 'Indicates returned quantity';
COMMENT ON COLUMN fds_cpg.rpt_cpg_monthly_royalty.returned_sales IS 'Indicates returned sales';
COMMENT ON COLUMN fds_cpg.rpt_cpg_monthly_royalty.refunded_quantity IS 'Indicates refunded quantity';
COMMENT ON COLUMN fds_cpg.rpt_cpg_monthly_royalty.refunded_sales IS 'Indicates refunded sales';
COMMENT ON COLUMN fds_cpg.rpt_cpg_monthly_royalty.etl_batch_id IS 'Unique ID of DBT Job used to insert the record';
COMMENT ON COLUMN fds_cpg.rpt_cpg_monthly_royalty.etl_insert_user_id IS 'Unique ID of the DBT user that was used to insert the record';
COMMENT ON COLUMN fds_cpg.rpt_cpg_monthly_royalty.etl_insert_rec_dttm IS 'Date Time information on when the DBT inserted the record';
COMMENT ON COLUMN fds_cpg.rpt_cpg_monthly_royalty.etl_update_user_id IS 'Unique ID of the DBT user which was used to update the record manually';
COMMENT ON COLUMN fds_cpg.rpt_cpg_monthly_royalty.etl_update_rec_dttm IS 'Date Time information on when the record was updated';    	







