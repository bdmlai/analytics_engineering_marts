-----------------------------------------------------------------------------------------------------------------------------------------------------------
Jira PSTA-1045 :
Table: (sql files under <project_home>/data_model/ddl/fds_nplus) -- review _comments,_ddl,_dml
	fds_nplus.rpt_nplus_daily_live_streams
	fds_nplus.rpt_nplus_daily_milestone_complete_rates
	fds_nplus.rpt_nplus_daily_nxt_tko_streams
	fds_nplus.rpt_nplus_daily_ppv_streams
--------------------------------------------------------------------------------------------------------------------------------------------------------------
PSTA-1392: All are views code available under 
							<project_home>/models/fds_fbk/vw_rpt_daily_fb_published_post.sql vw_rpt_daily_fb_published_video.sql
							<project_home>/models/fds_igm/vw_rpt_daily_ig_published_frame.sql vw_rpt_daily_ig_published_post.sql vw_rpt_daily_ig_published_story.sql
							<project_home>/models/fds_sc/vw_rpt_daily_sc_published_frame.sql vw_rpt_daily_sc_published_story.sql
							<project_home>/models/fds_sc/vw_rpt_daily_tw_published_post.sql
							
Views:						
	fds_fbk.vw_rpt_daily_fb_published_post
	fds_fbk.vw_rpt_daily_fb_published_video	
	fds_igm.vw_rpt_daily_ig_published_frame
	fds_igm.vw_rpt_daily_ig_published_post
	fds_igm.vw_rpt_daily_ig_published_story
	fds_sc.vw_rpt_daily_sc_published_frame
	fds_sc.vw_rpt_daily_sc_published_story
	fds_tw.vw_rpt_daily_tw_published_post	
-------------------------------------------------------------------------------------------------------------------------------------------	
PSTA-1407 (Code logic change ) model file under <project_home>/custom/4a/rpt_cp_monthly_global_consumption_tv.sql
	Table:
		fds_cp.rpt_cp_monthly_global_consumption_by_platform
-------------------------------------------------------------------------------------------------------------------------------------------
PSTA-1506 (Data fix )	code available under <project_home>/data_model/fds_cp/aggr_cp_weekly_consumption_by_platform_fix_dml.sql
	DML:
	    aggr_cp_weekly_consumption_by_platform_fix_dml.sql
	
-------------------------------------------------------------------------------------------------------------------------------------------	
BIOPS-198 : (Code change) code available under  <project_home>/models/fds_cp/rpt_cp_weekly_consolidated_kpi.sql
-------------------------------------------------------------------------------------------------------------------------------------------
