-- Switch behavior absolute value ranking view

/*
*************************************************************************************************************************************************
   Date        : 06/19/2020
   Version     : 1.0
   TableName   : vw_rpt_nl_weekly_channel_switch
   Schema	   : fds_nl
   Contributor : Hima Dasan
   Description : vw_rpt_nl_weekly_channel_switch view consists of absolute stay ,absolute switch and ranking based on switch for WWE, AEW and other wrestling programs (Weekly)
*************************************************************************************************************************************************
*/



select broadcast_Date,coverage_area,src_market_break,src_demographic_group
,src_broadcast_network_name,time_minute,mc_us_aa000,
absolute_stay,stay_percent,absolute_switch,switch_percent,
switch_percent_rank
 from "entdwdb"."fds_nl"."rpt_nl_weekly_channel_switch"  A