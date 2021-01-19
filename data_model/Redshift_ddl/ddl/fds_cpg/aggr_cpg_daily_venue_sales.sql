/*
************************************************************************************ 
Date : 09/23/2020
Version : 1.0
ViewName : aggr_cpg_daily_venue_sales
Schema : fds_cpg
Contributor : Rahul Chandran,Sudhakar Andugula
Frequency : Daily ; 10:00 A.M EST
Description : Aggregated CPG Daily Venue Sales Table consist of Sales details of WWE products on venue & event - basis
**************************************************************************************/
/* date      | by      |    Details   */

CREATE TABLE
    fds_cpg.aggr_cpg_daily_venue_sales
    (
        dim_agg_sales_id BIGINT ENCODE AZ64,
        date_id BIGINT ENCODE DELTA,
        dim_item_id BIGINT ENCODE DELTA32K,
        dim_event_id BIGINT ENCODE BYTEDICT,
        dim_venue_id BIGINT ENCODE BYTEDICT,
        quantity_shipped INTEGER ENCODE BYTEDICT,
        quantity_adjustment INTEGER ENCODE DELTA,
        quantity_returned INTEGER ENCODE BYTEDICT,
        compelements INTEGER ENCODE DELTA,
        net_units_sold INTEGER ENCODE BYTEDICT,
        selling_price DOUBLE PRECISION ENCODE BYTEDICT,
        total_revenue NUMERIC(38,10) ENCODE DELTA32K,
        complement_revenue NUMERIC(38,10) ENCODE BYTEDICT,
        active_flag CHARACTER VARYING(3) ENCODE LZO,
        effective_start_datetime TIMESTAMP WITHOUT TIME ZONE ENCODE DELTA32K,
        effective_end_datetime TIMESTAMP WITHOUT TIME ZONE ENCODE DELTA32K,
        etl_batch_id BIGINT ENCODE LZO,
        etl_insert_user_id CHARACTER VARYING(50) ENCODE LZO,
        etl_insert_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE DELTA32K,
        etl_update_user_id CHARACTER VARYING(50) ENCODE LZO,
        etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE DELTA32K
    )
    DISTSTYLE KEY DISTKEY
    (
        date_id
    )
    COMPOUND SORTKEY
    (
        date_id,
        dim_item_id
    );
COMMENT ON TABLE fds_cpg.aggr_cpg_daily_venue_sales
IS
    'Aggregated CPG Venue Sales Table consist of Sales details of WWE products on venue & event basis '
    ;
COMMENT ON COLUMN fds_cpg.aggr_cpg_daily_venue_sales.dim_agg_sales_id
IS
    'Auto generated id to uniquely identify a sale';
COMMENT ON COLUMN fds_cpg.aggr_cpg_daily_venue_sales.date_id
IS
    'Order Date';
COMMENT ON COLUMN fds_cpg.aggr_cpg_daily_venue_sales.dim_item_id
IS
    'References dim_item_id from dim_cpg_item table';
COMMENT ON COLUMN fds_cpg.aggr_cpg_daily_venue_sales.dim_event_id
IS
    'References dim_event_id from cdm.dim_event table';
COMMENT ON COLUMN fds_cpg.aggr_cpg_daily_venue_sales.dim_venue_id
IS
    'References dim_venue_id from fds_le.brdg_live_event_venue table';
COMMENT ON COLUMN fds_cpg.aggr_cpg_daily_venue_sales.quantity_shipped
IS
    'Shipped Quantity';
COMMENT ON COLUMN fds_cpg.aggr_cpg_daily_venue_sales.quantity_adjustment
IS
    'Adjusted Quantity';
COMMENT ON COLUMN fds_cpg.aggr_cpg_daily_venue_sales.quantity_returned
IS
    'Returned Quantity';
COMMENT ON COLUMN fds_cpg.aggr_cpg_daily_venue_sales.compelements
IS
    'Complements';
COMMENT ON COLUMN fds_cpg.aggr_cpg_daily_venue_sales.net_units_sold
IS
    'Net units sold';
COMMENT ON COLUMN fds_cpg.aggr_cpg_daily_venue_sales.selling_price
IS
    'Selling Price';
COMMENT ON COLUMN fds_cpg.aggr_cpg_daily_venue_sales.total_revenue
IS
    'Total revenue';
COMMENT ON COLUMN fds_cpg.aggr_cpg_daily_venue_sales.complement_revenue
IS
    'Complement revenue';
COMMENT ON COLUMN fds_cpg.aggr_cpg_daily_venue_sales.active_flag
IS
    'To indicate whether its active or not';
COMMENT ON COLUMN fds_cpg.aggr_cpg_daily_venue_sales.effective_start_datetime
IS
    'Effective start date time';
COMMENT ON COLUMN fds_cpg.aggr_cpg_daily_venue_sales.effective_end_datetime
IS
    'Effective end date time';
COMMENT ON COLUMN fds_cpg.aggr_cpg_daily_venue_sales.etl_batch_id
IS
    'Unique ID of DBT Job used to insert the record';
COMMENT ON COLUMN fds_cpg.aggr_cpg_daily_venue_sales.etl_insert_user_id
IS
    'Unique ID of the DBT user that was used to insert the record';
COMMENT ON COLUMN fds_cpg.aggr_cpg_daily_venue_sales.etl_insert_rec_dttm
IS
    'Date Time information on when the DBT inserted the record';
COMMENT ON COLUMN fds_cpg.aggr_cpg_daily_venue_sales.etl_update_user_id
IS
    'Unique ID of the DBT user which was used to update the record';
COMMENT ON COLUMN fds_cpg.aggr_cpg_daily_venue_sales.etl_update_rec_dttm
IS
    'Date Time information on when the record was updated';