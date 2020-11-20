/*
*******************************************************************************************
Date : 10/27/2020
Version : 1.0
TableName : drvd_intra_hour_quarter_hour_adds
Schema : fds_nplus
Contributor : Sudhakar Andugula
Description : Table consisting of paid adds trial adds live data for each hour 
JIRA : DED-3317
***************************************************************************************************
Updates
TYPE JIRA DEVELOPER DATE DESCRIPTION
----- --------- ----- -----------
NEW DED-3317 Sudhakar Andugula 11/20/2020 Initial Version  */
CREATE TABLE
    fds_nplus.drvd_intra_hour_quarter_hour_adds
    (
        DATE DATE ENCODE AZ64,
        hour INTEGER ENCODE AZ64,
        quarter_hour CHARACTER VARYING(14) ENCODE LZO,
        paid_adds BIGINT ENCODE AZ64,
        trial_adds BIGINT ENCODE AZ64,
        etl_insert_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        payment_method CHARACTER VARYING(255) ENCODE LZO
    );