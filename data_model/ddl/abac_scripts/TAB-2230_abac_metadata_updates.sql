/*
New job ids created :
CPG 1410
KNTR Job_id= 1107
NW2.0 705
Nielsen 1103
MKT 2002
Titok 1815

/*------------------------ Update job id for all DBT job to map to respective pod---------------------------------*/
---------------------------------------------------------------------------------------------

---CPG 5B  Application 14 , New job to be created "Data Mart Load  1410"

insert into abac.job(app_id,job_id,job_name,comments,created_by,created_date,updated_by,update_date)
values
(14,1410,'Data Mart Load','Job created for Data mart load CPG aggregate and report tables','bi_dbt_user_prd',sysdate(),null,null);

update abac.process set job_id=1410 ,process_description='Report table for CPG Appgregate Daily Kit sales',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_aggr_cpg_daily_kit_sales';
update abac.process set job_id=1410 ,process_description='Report table for CPG Appgregate Daily sales',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_aggr_cpg_daily_sales';
update abac.process set job_id=1410 ,process_description='Report table for CPG Appgregate Daily venue sales',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_aggr_cpg_daily_venue_sales';

update abac.process_config set process_scheduled_time='07:30:00',process_expected_completion_time='11:30:00',
process_sla='12:30:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_aggr_cpg_daily_kit_sales');

update abac.process_config set process_scheduled_time='07:30:00',process_expected_completion_time='11:30:00',
process_sla='12:30:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_aggr_cpg_daily_sales');

update abac.process_config set process_scheduled_time='07:30:00',
process_expected_completion_time='11:30:00',process_sla='12:30:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_aggr_cpg_daily_venue_sales');

------------------------------------------------------------------------------------------------------------------------------------

--KNTR  Application 22 ,New job to be created  "Data Mart Load"  Job_id= 1107

insert into abac.job(app_id,job_id,job_name,comments,created_by,created_date,updated_by,update_date)
values
(22,1107,'Data Mart Load','Job created for Data mart load KNTR aggregate and report tables','bi_dbt_user_prd',sysdate(),null,null);

update abac.process set job_id=1107 ,process_description='Aggr table for  KNTR weekly competetive program ratings',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_aggr_kntr_weekly_competitive_program_ratings';
update abac.process set job_id=1107 ,process_description='Report table for KNTR schedule VH data',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_kntr_schedule_vh_data';

update abac.process_config set process_scheduled_time='22:30:00',
process_expected_completion_time='00:00:00',process_sla='00:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_aggr_kntr_weekly_competitive_program_ratings');

update abac.process_config set process_scheduled_time='22:30:00',
process_expected_completion_time='00:00:00',process_sla='00:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_kntr_schedule_vh_data');

-------------------------------------------------------------------------------------------------------------------------------------

--NW2.0  Application 7 , New job to be created  "Data Mart Load"  Job_id= 705
insert into abac.job(app_id,job_id,job_name,comments,created_by,created_date,updated_by,update_date)
values
(7,705,'Data Mart Load','Job created for Data mart load NW2.0 aggregate and report tables','bi_dbt_user_prd',sysdate(),null,null);

update abac.process set job_id=705 ,process_description='Report table for PPV live plus vod for nxt event',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_nl_network_ppv_liveplusvod_nxt';
update abac.process set job_id=705 ,process_description='Report table for PPV live plus vod for ppv event',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_nl_network_ppv_liveplusvod_ppv';
update abac.process set job_id=705 ,process_description='Report table for PPV live plus vod for hof event',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_nl_ntwrk_ppv_liveplusvod_hof';
update abac.process set job_id=705 ,process_description='Report table for nplus daily live streams',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_nplus_daily_live_streams';
update abac.process set job_id=705 ,process_description='Report table for nplus daily milestone complete rates',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_nplus_daily_milestone_complete_rates';
update abac.process set job_id=705 ,process_description='Report table for nplus daily nxt tko streams',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_nplus_daily_nxt_tko_streams';
update abac.process set job_id=705 ,process_description='Report table for nplus daily ppv streams',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_nplus_daily_ppv_streams';
update abac.process set job_id=705 ,process_description='Aggr table for  monthly network kpis vkm',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_aggr_monthly_network_kpis_vkm';
update abac.process set job_id=705 ,process_description='Report table for cross platform daily int kpi ranking dotcom',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_cp_daily_int_kpi_ranking_dotcom';
update abac.process set job_id=705 ,process_description='Report table for cross platform daily int kpi ranking network',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_cp_daily_int_kpi_ranking_network';

update abac.process_config set process_scheduled_time='09:00:00',
process_expected_completion_time='11:00:00',process_sla='12:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_cp_daily_int_kpi_ranking_dotcom');

update abac.process_config set process_scheduled_time='11:00:00',
process_expected_completion_time='13:00:00',process_sla='14:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_cp_daily_int_kpi_ranking_network');


update abac.process_config set process_scheduled_time='09:00:00',
process_expected_completion_time='11:00:00',process_sla='12:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate(),comments='7' where process_id=(select process_id from abac.process 
where process_name='dbt_aggr_monthly_network_kpis_vkm');

update abac.process_config set process_scheduled_time='08:00:00',
process_expected_completion_time='10:00:00',process_sla='11:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_nplus_daily_ppv_streams');


update abac.process_config set process_scheduled_time='18:30:00',
process_expected_completion_time='20:00:00',process_sla='21:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_nl_network_ppv_liveplusvod_nxt');

update abac.process_config set process_scheduled_time='18:30:00',
process_expected_completion_time='20:00:00',process_sla='21:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_nl_network_ppv_liveplusvod_ppv');

update abac.process_config set process_scheduled_time='18:30:00',
process_expected_completion_time='20:00:00',process_sla='21:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_nl_ntwrk_ppv_liveplusvod_hof');

update abac.process_config set process_scheduled_time='08:00:00',
process_expected_completion_time='10:00:00',process_sla='11:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_nplus_daily_live_streams');

update abac.process_config set process_scheduled_time='08:30:00',
process_expected_completion_time='10:30:00',process_sla='11:30:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_nplus_daily_milestone_complete_rates');

update abac.process_config set process_scheduled_time='08:00:00',
process_expected_completion_time='10:00:00',process_sla='11:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_nplus_daily_nxt_tko_streams');


------------------------------------------------------------------------------------------------------------------------------------------

--Nielsen Application 11 -- New job to be created  1103 job  Data Mart load

insert into abac.job(app_id,job_id,job_name,comments,created_by,created_date,updated_by,update_date)
values
(11,1103,'Data Mart Load','Job created for Data mart load Nielsen aggregate and report tables','bi_dbt_user_prd',sysdate(),null,null);

update abac.process set job_id=1103 ,process_description='Report table for nielsen daily minxmin lite log ratings',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_nl_daily_minxmin_lite_log_ratings';
update abac.process set job_id=1103 ,process_description='Report table for nl daily wwe program ratings daily except tuesday',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_nl_daily_wwe_program_ratings_daily';
update abac.process set job_id=1103 ,process_description='Report table for nl daily wwe program ratings tuesday',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_nl_daily_wwe_program_ratings_tuesday';
update abac.process set job_id=1103 ,process_description='Report table for nl weekly channel switch',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_nl_weekly_channel_switch';
update abac.process set job_id=1103 ,process_description='Report table for nl weekly overlap derived way oob',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_nl_weekly_overlap_derived_4_way_oob';
update abac.process set job_id=1103 ,process_description='Report table for tv weekly consolidated kpi',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_tv_weekly_consolidated_kpi';
update abac.process set job_id=1103 ,process_description='Report table for nielsen wwe live quarter hour ratings daily',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_nl_daily_wwe_live_quarterhour_ratings_daily';
update abac.process set job_id=1103 ,process_description='Report table for daily wwe quarter hour ratings tuesday',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_nl_daily_wwe_live_quarterhour_ratings_tuesday';
update abac.process set job_id=1103 ,process_description='Report table for weekly live commercial',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_nl_weekly_wwe_live_commercial';

update abac.process set job_id=1103 ,process_description='Report table for cross platform monthly global consumption platform tv',updated_by='bi_dbt_user_prd',update_date=sysdate(), where process_name='dbt_rpt_cp_monthly_global_consumption_tv';
update abac.process set job_id=1103 ,process_description='Report table for cross platform daily int kpi ranking tv',updated_by='bi_dbt_user_prd',update_date=sysdate(), where process_name='dbt_rpt_cp_daily_int_kpi_ranking_tv';

update abac.process_config set process_scheduled_time='06:30:00',
process_expected_completion_time='08:30:00',process_sla='09:30:00',updated_by='bi_dbt_user_prd',update_date=sysdate(),comments='10,25' where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_cp_monthly_global_consumption_tv');

update abac.process_config set process_scheduled_time='22:30:00',
process_expected_completion_time='00:30:00',process_sla='01:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_cp_daily_int_kpi_ranking_tv');


update abac.process_config set process_scheduled_time='00:00:00',
process_expected_completion_time='02:00:00',process_sla='03:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_nl_daily_wwe_program_ratings_daily');

update abac.process_config set process_scheduled_time='00:00:00',
process_expected_completion_time='02:00:00',process_sla='03:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_nl_daily_wwe_program_ratings_tuesday');


update abac.process_config set process_scheduled_time='00:00:00',
process_expected_completion_time='02:00:00',process_sla='03:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_nl_weekly_channel_switch');

update abac.process_config set process_scheduled_time='22:00:00',
process_expected_completion_time='00:00:00',process_sla='01:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate(),comments='Monday' where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_nl_weekly_overlap_derived_4_way_oob');

update abac.process_config set process_scheduled_time='00:00:00',
process_expected_completion_time='02:00:00',process_sla='03:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_nl_daily_wwe_live_quarterhour_ratings_daily');

update abac.process_config set process_scheduled_time='00:00:00',
process_expected_completion_time='02:00:00',process_sla='03:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_nl_daily_wwe_live_quarterhour_ratings_tuesday');

update abac.process_config set process_scheduled_time='04:00:00',
process_expected_completion_time='06:00:00',process_sla='07:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate(),comments='Tuesday' where process_id=(select process_id from abac.process 
where process_name='dbt_nl_weekly_wwe_live_commercial');

update abac.process_config set process_scheduled_time='14:00:00',
process_expected_completion_time='16:00:00',process_sla='17:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_nl_daily_minxmin_lite_log_ratings');

update abac.process_config set process_scheduled_time='11:00:00',
process_expected_completion_time='12:00:00',process_sla='12:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_tv_weekly_consolidated_kpi');

----------------------------------------------------------------------------------------------------------------------------------------------------------

--mkt  Application-- 27 , New Job 2002 Job "Data mart load"

insert into abac.job(app_id,job_id,job_name,comments,created_by,created_date,updated_by,update_date)
values
(27,2002,'Data Mart Load','Job created for Data mart load Nielsen aggregate and report tables','bi_dbt_user_prd',sysdate(),null,null);

update abac.process set job_id=2002 ,process_description='Report table for market weekly owned media execution',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_mkt_weekly_owned_media_execution';
update abac.process set job_id=2002 ,process_description='Report table for market weekly paid media execution',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_mkt_weekly_paid_media_execution';

update abac.process_config set process_scheduled_time='09:00:00',
process_expected_completion_time='11:00:00',process_sla='12:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate(),comments='Monday' where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_mkt_weekly_owned_media_execution');

update abac.process_config set process_scheduled_time=,
process_expected_completion_time=,process_sla=,updated_by='bi_dbt_user_prd',update_date=sysdate(),,comments='Monday' where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_mkt_weekly_paid_media_execution');

------------------------------------------------------------------------------------------------------------------------------------------------

-- Youtube Application 4,Map Job ID 404	Data Mart Load

update abac.process set job_id=404 ,process_description='Report table for youtube daily consumption by channel',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_yt_daily_consumption_bychannel';

update abac.process_config set process_scheduled_time='08:00:00',
process_expected_completion_time='10:00:00',process_sla='11:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_yt_daily_consumption_bychannel');


update abac.process set job_id=404 ,process_description='Report table for cross platform daily int kpi ranking youtube',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_cp_daily_int_kpi_ranking_youtube';

update abac.process_config set process_scheduled_time='08:00:00',
process_expected_completion_time='10:00:00',process_sla='11:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_cp_daily_int_kpi_ranking_youtube');

-----------------------------------------------------------------------------------------------------------------------------------------------

-- Conviva Application 10 , Map Job_id 1013
update abac.process set job_id=1013 ,process_description='Report table for cross platform daily followership by platform digitalsnapshot',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_cp_daily_followership_by_platform_digitalsnapshot';
update abac.process set job_id=1013 ,process_description='Report table for cross platform monthly global followership by platform',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_cp_monthly_global_followership_by_platform';
update abac.process set job_id=1013 ,process_description='Report table for cross platform monthly social overview',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_cp_monthly_social_overview';
update abac.process set job_id=1013 ,process_description='Report table for cross platform weekly consolidated kpi',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_cp_weekly_consolidated_kpi';
update abac.process set job_id=1013 ,process_description='Report table for cross platform daily social followership',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_cp_daily_social_followership';
update abac.process set job_id=1013 ,process_description='Report table for cross platform weekly consumption by platform',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_cp_weekly_consumption_by_platform';
update abac.process set job_id=1013 ,process_description='Report table for cross platform monthly global consumption platform 5th day',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_cp_monthly_global_consumption_platform_5th';
update abac.process set job_id=1013 ,process_description='Report table for cross platform weekly social followership',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_cp_weekly_social_followership';


update abac.process_config set process_scheduled_time='14:00:00',
process_expected_completion_time='16:00:00',process_sla='17:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_cp_daily_followership_by_platform_digitalsnapshot');

update abac.process_config set process_scheduled_time='10:00:00',
process_expected_completion_time='12:00:00',process_sla='13:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate(),comments='5' where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_cp_monthly_global_followership_by_platform');

update abac.process_config set process_scheduled_time='09:00:00',
process_expected_completion_time='11:00:00',process_sla='12:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate(),comments='5' where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_cp_monthly_social_overview');

update abac.process_config set process_scheduled_time='11:00:00',
process_expected_completion_time='13:00:00',process_sla='14:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate(),comments='Wednesday' where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_cp_weekly_consolidated_kpi');

update abac.process_config set process_scheduled_time='10:00:00',
process_expected_completion_time='12:00:00',process_sla='13:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_cp_daily_social_followership');

update abac.process_config set process_scheduled_time='10:30:00',
process_expected_completion_time='12:30:00',process_sla='13:30:00',updated_by='bi_dbt_user_prd',update_date=sysdate(),comments='Wednesday' where process_id=(select process_id from abac.process 
where process_name='dbt_cp_weekly_consumption_by_platform');


update abac.process_config set process_scheduled_time='11:00:00',
process_expected_completion_time='13:00:00',process_sla='14:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate(),comments='5' where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_cp_monthly_global_consumption_platform_5th');

update abac.process_config set process_scheduled_time='09:00:00',
process_expected_completion_time='11:00:00',process_sla='12:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate(),comments='Monday' where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_cp_weekly_social_followership');

------------------------------------------------------------------------------------------------------------------------------------------------------------------

--Tiktok Application 18, new job 1815 created
insert into abac.job(app_id,job_id,job_name,comments,created_by,created_date,updated_by,update_date)
values
(18,1815,'Data Mart Load','Job created for Data mart load Tiktok aggregate and report tables','bi_dbt_user_prd',sysdate(),null,null);

update abac.process set job_id=1815 ,process_description='Report table for cross platform monthly global consumption platform tiktok',updated_by='bi_dbt_user_prd',update_date=sysdate() where process_name='dbt_rpt_cp_monthly_global_consumption_tiktok';

update abac.process_config set process_scheduled_time='06:00:00',
process_expected_completion_time='08:00:00',process_sla='09:00:00',updated_by='bi_dbt_user_prd',update_date=sysdate(),comments='5,7' where process_id=(select process_id from abac.process 
where process_name='dbt_rpt_cp_monthly_global_consumption_tiktok');

---------------------------------------------------------------------------------------------------------------------------------------------------------------


-- Delete DBT entry from applications tables
delete from abac.application where app_id=31 and app_name='DBT';
------------------------------------------------------------------------------------------
-- Update job id for all DBT job to map to respective pod

delete from abac.job where job_id=31001 and job_name='DBT_aggr_report_table_job';
