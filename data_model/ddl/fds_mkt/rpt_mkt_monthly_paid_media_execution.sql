/*
************************************************************************************ 
    Date: 10/02/2020
    Version: 1.0
    Description: First Draft
    Contributor: Lakshman M Murugeshan
	Description - Aggregate table for paid media content data for MMM inputs.
	Metrics include Impressions, Spend and Clicks for channels such as Display, Search, Youtube, Facebook, Twitter and Snapchat
	Schedule Detail: Frequency - Monthly
    Adl' notes:  
    updated comments
**************************************************************************************/
/* date      | by      |    Details   */

CREATE TABLE
    fds_mkt.rpt_mkt_monthly_paid_media_execution
    (
        week DATE,
        country CHARACTER VARYING(7) ENCODE LZO,
        vehicle CHARACTER VARYING(12) ENCODE LZO,
        level2 CHARACTER VARYING(250) ENCODE LZO,
        level3 CHARACTER VARYING(250) ENCODE LZO,
        metric CHARACTER VARYING(11) ENCODE LZO,
        impressions BIGINT ENCODE AZ64,
        spend NUMERIC(38,12) ENCODE AZ64,
        clicks BIGINT ENCODE AZ64,
        data_category CHARACTER VARYING(10) ENCODE LZO,
        audience CHARACTER VARYING(3) ENCODE LZO,
        ppv_name CHARACTER VARYING(256) ENCODE LZO,
        ppv_type CHARACTER VARYING(256) ENCODE LZO,
        etl_batch_id CHARACTER VARYING(29) ENCODE LZO,
        etl_insert_user_id CHARACTER VARYING(15) ENCODE LZO,
        etl_insert_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        etl_update_user_id CHARACTER VARYING(1) ENCODE LZO,
        etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64
    )
    DISTSTYLE KEY DISTKEY
    (
        week
    )
    COMPOUND SORTKEY
    (
        week
    );
	
	
COMMENT ON COLUMN fds_mkt.rpt_mkt_monthly_paid_media_execution.week 
IS 'calender week ending Sunday date';

COMMENT ON COLUMN fds_mkt.rpt_mkt_monthly_paid_media_execution.country 
IS 'Country names grouped as USA, ROW, AUS/NZ, UK/IRE, GERMANY, Global';
COMMENT ON COLUMN fds_mkt.rpt_mkt_monthly_paid_media_execution.vehicle 
IS 'Hierarchy level column which takes the values Paid Social, Paid Display, Paid Search, Paid Youtube';
COMMENT ON COLUMN fds_mkt.rpt_mkt_monthly_paid_media_execution.level2 
IS 'Hierarchy level column which takes the values Nurture, Paid Snapchat, Conversion, Youtube Trueview, Paid Facebook, adtheorent, Paid Twitter, Bing, Google';
COMMENT ON COLUMN fds_mkt.rpt_mkt_monthly_paid_media_execution.level3 
IS 'Hierarchy level column which takes the values adtheorent, captify, draftkings, Facebook, fandom, miq, programmaticmechanics, samba, Snapchat, Twitter, vdx, xaxisdirect, yieldmo';
COMMENT ON COLUMN fds_mkt.rpt_mkt_monthly_paid_media_execution.metric 
IS 'name of the metric';
COMMENT ON COLUMN fds_mkt.rpt_mkt_monthly_paid_media_execution.impressions 
IS 'impressions';
COMMENT ON COLUMN fds_mkt.rpt_mkt_monthly_paid_media_execution.spend 
IS 'spend';
COMMENT ON COLUMN fds_mkt.rpt_mkt_monthly_paid_media_execution.clicks 
IS 'clicks';

COMMENT ON COLUMN fds_mkt.rpt_mkt_monthly_paid_media_execution.data_category 
IS 'Hardcoded as Paid Media';

COMMENT ON COLUMN fds_mkt.rpt_mkt_monthly_paid_media_execution.audience  
IS 'Hardcoded as All';

COMMENT ON COLUMN fds_mkt.rpt_mkt_monthly_paid_media_execution.ppv_name 
IS 'name of the ppv event';
COMMENT ON COLUMN fds_mkt.rpt_mkt_monthly_paid_media_execution.ppv_type 
IS 'ppv event type';
	
	