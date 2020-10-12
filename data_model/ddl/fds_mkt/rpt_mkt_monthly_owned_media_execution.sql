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
    fds_mkt.rpt_mkt_monthly_owned_media_execution
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
	
	COMMENT ON COLUMN fds_mkt.rpt_mkt_monthly_owned_media_execution.week 
IS 'calender week ending Sunday date';

	COMMENT ON COLUMN fds_mkt.rpt_mkt_monthly_owned_media_execution.country 
IS 'Country names grouped as ROW, AUS/NZ, UK/IRE, GERMANY, Global';

	COMMENT ON COLUMN fds_mkt.rpt_mkt_monthly_owned_media_execution.vehicle 
IS 'hierarchy level column which takes the values Owned TV, Owned Youtube, Owned Social';

	COMMENT ON COLUMN fds_mkt.rpt_mkt_monthly_owned_media_execution.level2 
IS 'hierarchy level column which takes the values Owned TV Viewership, EndScreen, Annotations, Cards, Network, Lower Third, Promo Graphic, Promo';

	COMMENT ON COLUMN fds_mkt.rpt_mkt_monthly_owned_media_execution.level3 
IS 'hierarchy level column which takes the values RAW, PPV Kick-Off, SMACKDOWN, PPV, Twitter, Facebook, NXT, Instagram';

	COMMENT ON COLUMN fds_mkt.rpt_mkt_monthly_owned_media_execution.metric 
IS 'name of the metric';

	COMMENT ON COLUMN fds_mkt.rpt_mkt_monthly_owned_media_execution.exposure 
IS 'value of the metric';

	COMMENT ON COLUMN fds_mkt.rpt_mkt_monthly_owned_media_execution.data_category 
IS 'Hard coded as Owned Media';

	COMMENT ON COLUMN fds_mkt.rpt_mkt_monthly_owned_media_execution.audience 
IS 'Hard coded as All';

	COMMENT ON COLUMN fds_mkt.rpt_mkt_monthly_owned_media_execution.ppv_name 
IS 'name of the ppv event';

	COMMENT ON COLUMN fds_mkt.rpt_mkt_monthly_owned_media_execution.ppv_type 
IS 'ppv event type';
