
	COMMENT ON COLUMN fds_mkt.rpt_mkt_weekly_owned_media_execution.week 
IS 'calender week ending Sunday date';

	COMMENT ON COLUMN fds_mkt.rpt_mkt_weekly_owned_media_execution.country 
IS 'Country names grouped as ROW, AUS/NZ, UK/IRE, GERMANY, Global';

	COMMENT ON COLUMN fds_mkt.rpt_mkt_weekly_owned_media_execution.vehicle 
IS 'hierarchy level column which takes the values Owned TV, Owned Youtube, Owned Social';

	COMMENT ON COLUMN fds_mkt.rpt_mkt_weekly_owned_media_execution.level2 
IS 'hierarchy level column which takes the values Owned TV Viewership, EndScreen, Annotations, Cards, Network, Lower Third, Promo Graphic, Promo';

	COMMENT ON COLUMN fds_mkt.rpt_mkt_weekly_owned_media_execution.level3 
IS 'hierarchy level column which takes the values RAW, PPV Kick-Off, SMACKDOWN, PPV, Twitter, Facebook, NXT, Instagram';

	COMMENT ON COLUMN fds_mkt.rpt_mkt_weekly_owned_media_execution.metric 
IS 'name of the metric';

	COMMENT ON COLUMN fds_mkt.rpt_mkt_weekly_owned_media_execution.exposure 
IS 'value of the metric';

	COMMENT ON COLUMN fds_mkt.rpt_mkt_weekly_owned_media_execution.data_category 
IS 'Hard coded as Owned Media';

	COMMENT ON COLUMN fds_mkt.rpt_mkt_weekly_owned_media_execution.audience 
IS 'Hard coded as All';

	COMMENT ON COLUMN fds_mkt.rpt_mkt_weekly_owned_media_execution.ppv_name 
IS 'name of the ppv event';

	COMMENT ON COLUMN fds_mkt.rpt_mkt_weekly_owned_media_execution.ppv_type 
IS 'ppv event type';
