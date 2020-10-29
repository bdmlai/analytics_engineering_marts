/*
**********************************************************************************************
Date : 07/09/2020
Version : 2.0
ViewName : aggr_cp_weekly_consumption_by_platform
Schema : fds_cp
Contributor : Sandeep Battula
Frequency : Weekly Wednesday 10:30 AM
Description : aggr_cp_weekly_consumption_by_platform This aggregate table stores the crossplatform consumption metrics - total views and total minutes watched aggregated for each week for platforms- Youtube, facebook, Twitter, Instagram, Snapchat and dotcom/App.
*********************************************************************************************************************************/



comment on column fds_cp.aggr_cp_weekly_consumption_by_platform.platform
"stores the name of cross platform";

comment on column fds_cp.aggr_cp_weekly_consumption_by_platform.monday_date
"represents the start of the week for measurement period Monday to Sunday";

comment on column fds_cp.aggr_cp_weekly_consumption_by_platform.views
"indicates the total views for the week for each platform";

comment on column fds_cp.aggr_cp_weekly_consumption_by_platform.minutes_watched
"indicates the total minutes watched for the week for each platform";	

comment on column fds_cp.aggr_cp_weekly_consumption_by_platform.prev_views
"indicates the total minutes watched for the week for each platform";	


comment on column fds_cp.aggr_cp_weekly_consumption_by_platform.prev_mins
"indicates the total views for previous week for each platform"

comment on column fds_cp.aggr_cp_weekly_consumption_by_platform.weekly_per_change_views
"indicates the total minutes watched for previous week for each platform";

comment on column fds_cp.aggr_cp_weekly_consumption_by_platform.weekly_per_change_mins
"gives the week over week change in total views for each platform";

comment on column fds_cp.aggr_cp_weekly_consumption_by_platform.monday_date
"gives the week over week change in total minutes watched for each platform";

	
	