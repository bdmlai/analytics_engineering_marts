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

CREATE TABLE
    fds_cpg.rpt_cpg_monthly_royalty
    (
        source CHARACTER VARYING(100) ENCODE LZO,
        currency_type CHARACTER VARYING(30) ENCODE LZO,
        report_month date,
        month CHARACTER VARYING(50) ENCODE LZO,
        year SMALLINT ENCODE AZ64,
        quarter CHARACTER VARYING(50) ENCODE LZO,
        item_id CHARACTER VARYING(100) ENCODE LZO,
        item_description CHARACTER VARYING(75) ENCODE LZO,
        royalty_code CHARACTER VARYING(60) ENCODE LZO,
		shipped_quantity INTEGER ENCODE AZ64,
		shipped_sales  NUMERIC(28,4) ENCODE AZ64,
		returned_quantity INTEGER ENCODE AZ64,
		returned_sales  NUMERIC(28,4) ENCODE AZ64,
		refunded_quantity INTEGER ENCODE AZ64,
		refunded_sales  NUMERIC(15,4) ENCODE AZ64,
          etl_batch_id CHARACTER VARYING(30) ENCODE LZO,
          etl_insert_user_id CHARACTER VARYING(20) ENCODE LZO,
          etl_insert_rec_dttm TIMESTAMP WITH TIME ZONE ENCODE AZ64,
          etl_update_user_id CHARACTER VARYING(20) ENCODE LZO,
          etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64

    );


