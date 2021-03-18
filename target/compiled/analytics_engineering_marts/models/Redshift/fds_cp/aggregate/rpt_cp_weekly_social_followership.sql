--Weekly Social Followership

/*
*************************************************************************************************************************************************
   Date        : 11/04/2020
   Version     : 1.0
   TableName   : rpt_cp_weekly_social_followership
   Schema	   : fds_cp
   Contributor : Sandeep Battula
   Description : Weekly Social Followership reporting table consists of social followership data for conviva platforms and China social platforms. It also stores account_name, designation, brand, gender for the talent.  
*************************************************************************************************************************************************
*/



with #latest_emm_brand as 
(select * from (select character_wweid, designation, row_number() over (partition by character_wweid order by start_date desc) as row_num
from fds_emm.brand where (current_date - 6) between start_date and coalesce (end_date, current_date))
where row_num=1),

#latest_emm_designation as 
(select * from (select character_wweid, designation, row_number() over (partition by character_wweid order by start_date desc) as row_num
from fds_emm.babyface_heel where (current_date - 6) between start_date and coalesce (end_date, current_date))
where row_num=1),

#latest_week_social as 
(select distinct
'Facebook' as platform,
A.facebook_followers as followers,
B.account_name as account,
coalesce(F.designation, 'Other') as brand,
coalesce(E.designation, 'Other') as designation,
coalesce(D.gender, 'Unavailable') as gender,
G.full_date as as_on_date
from fds_cp.fact_co_smfollowership_cumulative_summary A
left join cdm.dim_smprovider_account B
on A.dim_smprovider_account_id = B.dim_smprovider_account_id
left join hive_udl_cp.da_monthly_conviva_emm_accounts_mapping C
on B.account_name = C.all_conviva_accounts
left join fds_mdm.character D
on coalesce (C.character_lineage,'NA') = coalesce (D.character_lineage_name,'NA')
and C.character_lineage <> ' '
and D.characters_name=C.all_conviva_accounts
Left Join #latest_emm_designation E 
on D.characters_wweid=e.character_wweid
left join #latest_emm_brand F
on D.characters_wweid=F.character_wweid
left join cdm.dim_date G
on A.dim_date_id = G.dim_date_id
where A.facebook_followers != 0
and G.day_of_week_nm = 'friday'
and to_date(A.as_on_date,'yyyymmdd') >= current_date - 7

union all

select 
'Instagram' as platform,
A.instagram_followers as followers,
B.account_name as account,
coalesce(F.designation, 'Other') as brand,
coalesce(E.designation, 'Other') as designation,
coalesce(D.gender, 'Unavailable') as gender,
G.full_date as as_on_date
from fds_cp.fact_co_smfollowership_cumulative_summary A
left join cdm.dim_smprovider_account B
on A.dim_smprovider_account_id = B.dim_smprovider_account_id
left join hive_udl_cp.da_monthly_conviva_emm_accounts_mapping C
on B.account_name = C.all_conviva_accounts
left join fds_mdm.character D
on coalesce (C.character_lineage,'NA') = coalesce (D.character_lineage_name,'NA')
and C.character_lineage <> ' '
and D.characters_name=C.all_conviva_accounts
Left Join #latest_emm_designation E 
on D.characters_wweid=e.character_wweid
left join #latest_emm_brand F
on D.characters_wweid=F.character_wweid
left join cdm.dim_date G
on A.dim_date_id = G.dim_date_id
where A.instagram_followers != 0
and G.day_of_week_nm = 'friday'
and to_date(A.as_on_date,'yyyymmdd') >= current_date - 7 

union all

select 
'Twitter' as platform,
A.twitter_followers as followers,
B.account_name as account,
coalesce(F.designation, 'Other') as brand,
coalesce(E.designation, 'Other') as designation,
coalesce(D.gender, 'Unavailable') as gender,
G.full_date as as_on_date
from fds_cp.fact_co_smfollowership_cumulative_summary A
left join cdm.dim_smprovider_account B
on A.dim_smprovider_account_id = B.dim_smprovider_account_id
left join hive_udl_cp.da_monthly_conviva_emm_accounts_mapping C
on B.account_name = C.all_conviva_accounts
left join fds_mdm.character D
on coalesce (C.character_lineage,'NA') = coalesce (D.character_lineage_name,'NA')
and C.character_lineage <> ' '
and D.characters_name=C.all_conviva_accounts
Left Join #latest_emm_designation E 
on D.characters_wweid=e.character_wweid
left join #latest_emm_brand F
on D.characters_wweid=F.character_wweid
left join cdm.dim_date G
on A.dim_date_id = G.dim_date_id
where A.twitter_followers != 0
and G.day_of_week_nm = 'friday'
and to_date(A.as_on_date,'yyyymmdd') >= current_date - 7

union all

select 
'Youtube' as platform,
A.youtube_subscribers as followers,
B.account_name as account,
coalesce(F.designation, 'Other') as brand,
coalesce(E.designation, 'Other') as designation,
coalesce(D.gender, 'Unavailable') as gender,
G.full_date as as_on_date
from fds_cp.fact_co_smfollowership_cumulative_summary A
left join cdm.dim_smprovider_account B
on A.dim_smprovider_account_id = B.dim_smprovider_account_id
left join hive_udl_cp.da_monthly_conviva_emm_accounts_mapping C
on B.account_name = C.all_conviva_accounts
left join fds_mdm.character D
on coalesce (C.character_lineage,'NA') = coalesce (D.character_lineage_name,'NA')
and C.character_lineage <> ' '
and D.characters_name=C.all_conviva_accounts
Left Join #latest_emm_designation E 
on D.characters_wweid=e.character_wweid
left join #latest_emm_brand F
on D.characters_wweid=F.character_wweid
left join cdm.dim_date G
on A.dim_date_id = G.dim_date_id
where A.youtube_subscribers != 0
and G.day_of_week_nm = 'friday'
and to_date(A.as_on_date,'yyyymmdd') >= current_date - 7

union all


select distinct
A.platform,
A.followers,
A.account_name as account,
coalesce(F.designation, 'Other') as brand,
coalesce(E.designation, 'Other') as designation,
coalesce(D.gender, 'Unavailable') as gender,
G.full_date as as_on_date
from hive_udl_cp.social_daily_followship_metrics A
left join hive_udl_cp.da_monthly_conviva_emm_accounts_mapping C
on A.account_name = C.all_conviva_accounts
left join fds_mdm.character D
on coalesce (C.character_lineage,'NA') = coalesce (D.character_lineage_name,'NA')
and D.characters_name=C.all_conviva_accounts
Left Join #latest_emm_designation E 
on D.characters_wweid=e.character_wweid
left join #latest_emm_brand F
on D.characters_wweid=F.character_wweid
left join cdm.dim_date G
on A.source_as_on_date = to_date(G.dim_date_id,'yyyymmdd')
where platform='Instagram' 
and A.followers != 0
and G.day_of_week_nm = 'friday'
and A.as_on_date = (select max(as_on_date) from hive_udl_cp.social_daily_followship_metrics)
and A.source_as_on_date >= current_date - 7

union all

-- This query fetches the metrics for platforms- Snapchat and TikTok

select distinct 
A.platform,
A.followers,
'WWE' as account,
'Other' as brand,
'Other' as designation,
'Unavailable' as gender,
B.full_date as as_on_date
from hive_udl_cp.social_daily_followship_metrics A
left join cdm.dim_date B 
on A.source_as_on_date = to_date(B.dim_date_id,'yyyymmdd')
where A.followers != 0
and platform IN ('Snapchat','TikTok')
and B.day_of_week_nm = 'friday'
and A.as_on_date = (select max(as_on_date) from hive_udl_cp.social_daily_followship_metrics)
and source_as_on_date >= current_date - 7

union all

-- Below code is for China Social platforms

select 
distinct
'Weibo' as platform,
weibo_total_followers as followers,
'WWE' as account,
'Other' as brand,
'Other' as designation,
'Unavailable' as gender,
to_date(as_on_date, 'yyyymmdd') as as_on_date
from hive_udl_chscl.china_weekly_social_data
where as_on_date = (select max(as_on_date) from hive_udl_chscl.china_weekly_social_data)
and end_date >= (current_date - 7)
and date_part(dow, end_date) = 4

union all

select 
distinct
'WeChat' as platform,
wechat_total_wechat_followers as followers,
'WWE' as account,
'Other' as brand,
'Other' as designation,
'Unavailable' as gender,
to_date(as_on_date, 'yyyymmdd') as as_on_date
from hive_udl_chscl.china_weekly_social_data
where as_on_date = (select max(as_on_date) from hive_udl_chscl.china_weekly_social_data)
and end_date >= (current_date - 7) and date_part(dow, end_date) = 4

union all

select 
distinct
'Douyin' as platform,
total_xia_douyin_followers as followers,
'Xia Li' as account,
'NXT' as brand,
'Face' as designation,
'Female' as gender,
to_date(as_on_date, 'yyyymmdd') as as_on_date
from hive_udl_chscl.china_weekly_social_data
where as_on_date = (select max(as_on_date) from hive_udl_chscl.china_weekly_social_data)
and end_date >= (current_date - 7) and date_part(dow, end_date) = 4

union all

select 
distinct
'Douyin' as platform,
total_boa_douyin_followers as followers,
'Xia Li' as account,
'NXT' as brand,
'Face' as designation,
'Male' as gender,
to_date(as_on_date, 'yyyymmdd') as as_on_date
from hive_udl_chscl.china_weekly_social_data
where as_on_date = (select max(as_on_date) from hive_udl_chscl.china_weekly_social_data)
and end_date >= (current_date - 7) and date_part(dow, end_date) = 4

union all

select 
distinct
'Qzone' as platform,
wechat_total_qzone_followers as followers,
'WWE' as account,
'Other' as brand,
'Other' as designation,
'Unavailable' as gender,
to_date(as_on_date, 'yyyymmdd') as as_on_date
from hive_udl_chscl.china_weekly_social_data
where as_on_date = (select max(as_on_date) from hive_udl_chscl.china_weekly_social_data)
and end_date >= (current_date - 7) and date_part(dow, end_date) = 4

union all

select 
distinct
'Toutiao' as platform,
toutiao_total_followers as followers,
'WWE' as account,
'Other' as brand,
'Other' as designation,
'Unavailable' as gender,
to_date(as_on_date, 'yyyymmdd') as as_on_date
from hive_udl_chscl.china_weekly_social_data
where as_on_date = (select max(as_on_date) from hive_udl_chscl.china_weekly_social_data)
and end_date >= (current_date - 7) and date_part(dow, end_date) = 4

union all

select 
distinct
'V+' as platform,
wechat_total_vplus_subscribers as followers,
'WWE' as account,
'Other' as brand,
'Other' as designation,
'Unavailable' as gender,
to_date(as_on_date, 'yyyymmdd') as as_on_date
from hive_udl_chscl.china_weekly_social_data
where as_on_date = (select max(as_on_date) from hive_udl_chscl.china_weekly_social_data)
and end_date >= (current_date - 7) and date_part(dow, end_date) = 4

union all

select 
distinct
'YouKu' as platform,
youku_total_followers as followers,
'WWE' as account,
'Other' as brand,
'Other' as designation,
'Unavailable' as gender,
to_date(as_on_date, 'yyyymmdd') as as_on_date
from hive_udl_chscl.china_weekly_social_data
where as_on_date = (select max(as_on_date) from hive_udl_chscl.china_weekly_social_data)
and end_date >= (current_date - 7) and date_part(dow, end_date) = 4

union all

select 
distinct
'QTT' as platform,
qtt_total_followers as followers,
'WWE' as account,
'Other' as brand,
'Other' as designation,
'Unavailable' as gender,
to_date(as_on_date, 'yyyymmdd') as as_on_date
from hive_udl_chscl.china_weekly_social_data
where as_on_date = (select max(as_on_date) from hive_udl_chscl.china_weekly_social_data)
and end_date >= (current_date - 7) and date_part(dow, end_date) = 4)


select a.*,
100001 		 as  etl_batch_id,
'bi_dbt_user_prd' as etl_insert_user_id,
sysdate 	 as etl_insert_rec_dttm,
'' 			 as etl_update_user_id,
sysdate 	 as etl_update_rec_dttm
from #latest_week_social a