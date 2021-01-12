/*
************************************************************************************ 
    Date: 10/02/2020
    Version: 1.0
    Description: First Draft
    Contributor: Lakshman M Murugeshan
	Description - Aggregate table for owned media content data for MMM inputs.
	Metrics include Impressions, Promos and Viewership (US, Non-US and PPV) for Owned TV channels such as Youtube, Facebook, Twitter and Instagram.
	Schedule Detail: Frequency - Monthly
    Adl' notes:  
    updated comments:
	
**************************************************************************************/
/* date      | by      |    Details   */

CREATE TABLE
    fds_mkt.rpt_mkt_weekly_owned_media_execution
    (
        week DATE,
        country CHARACTER VARYING(7) ENCODE LZO,
        vehicle CHARACTER VARYING(13) ENCODE LZO,
        level2 CHARACTER VARYING(300) ENCODE LZO,
        level3 CHARACTER VARYING(768) ENCODE LZO,
        metric CHARACTER VARYING(16) ENCODE LZO,
        exposure DOUBLE PRECISION,
        data_category CHARACTER VARYING(11) ENCODE LZO,
        audience CHARACTER VARYING(3) ENCODE LZO,
        ppv_name CHARACTER VARYING(256) ENCODE LZO,
        ppv_type CHARACTER VARYING(256) ENCODE LZO,
        etl_batch_id CHARACTER VARYING(29) ENCODE LZO,
        etl_insert_user_id CHARACTER VARYING(15) ENCODE LZO,
        etl_insert_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        etl_update_user_id CHARACTER VARYING(15) ENCODE LZO,
        etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64
    )
    COMPOUND SORTKEY
    (
        week
    );
	
