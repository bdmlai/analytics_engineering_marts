/*
*************************************************************************************************************************************************
   Date        : 06/19/2020
   Version     : 1.0
   TableName   : rpt_nl_weekly_overlap_derived_4_way_oob
   Schema	   : fds_nl
   Contributor : Remya K Nair
   Description : rpt_nl_weekly_overlap_derived_4_way_oob view consists of weekly  overlap program schedules and unique reach for each time period 
********
*/







with first_15_schedules as 
(select dim_date_id,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,
sum(case when schedule_name like '%A | SmackDown (FOX)%' then aa_reach_proj000 end) A,
sum(case when schedule_name like '%B | Raw (USA)%' then aa_reach_proj000 end) B,
sum(case when schedule_name like '%C | NXT (USA)%' then aa_reach_proj000 end) C,
sum(case when schedule_name like '%D | AEW (TNT)%' then aa_reach_proj000 end) D,

sum(case when schedule_name = 'AB' then aa_reach_proj000 end) AB,

sum(case when schedule_name = 'BC' then aa_reach_proj000 end) BC,

sum(case when schedule_name = 'CA' then aa_reach_proj000 end) CA,

sum(case when schedule_name = 'AD' then aa_reach_proj000 end) AD,

sum(case when schedule_name = 'BD' then aa_reach_proj000 end) BD,

sum(case when schedule_name = 'CD' then aa_reach_proj000 end) CD,

sum(case when schedule_name = 'ABC' then aa_reach_proj000 end) ABC,

sum(case when schedule_name = 'ABD' then aa_reach_proj000 end) ABD,

sum(case when schedule_name = 'BCD' then aa_reach_proj000 end) BCD,

sum(case when schedule_name = 'ACD' then aa_reach_proj000 end) ACD,

sum(case when schedule_name = 'ABCD' then aa_reach_proj000 end) ABCD,

max(aa_reach_proj000) as Max_AA_Reac_Proj_000 
FROM   fds_nl.fact_nl_weekly_overlap_4_way_oob a
where a.etl_insert_rec_dttm  >  coalesce ((select max(etl_insert_rec_dttm) from "entdwdb"."fds_nl"."rpt_nl_weekly_overlap_derived_4_way_oob"), '1900-01-01 00:00:00') 
--where dim_date_id='20200302'
GROUP BY 1,2,3,4,5),
 total_schedules as 
(
select dim_date_id,'Derived' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'Both A&B' as schedule_name ,
(A+B-AB) as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'Total Combined SmackDown and Raw' as overlap_description  from first_15_schedules
union all

select dim_date_id,'Derived' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'Both A&C' as schedule_name ,
(A+C-CA) as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'Total Combined SmackDown and NXT' as overlap_description  from first_15_schedules
union all

select dim_date_id,'Derived' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'Both A&D' as schedule_name ,
(A+D-AD) as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'Total Combined SmackDown and AEW' as overlap_description  from first_15_schedules
union all

select dim_date_id,'Derived' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'Both B&D' as schedule_name ,
(B+D-BD) as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'Total Combined Raw and AEW' as overlap_description  from first_15_schedules
union all

select dim_date_id,'Derived' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'Both B&C' as schedule_name ,
(B+C-BC) as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'Total Combined Raw and NXT' as overlap_description  from first_15_schedules
union all

select dim_date_id,'Derived' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'Both C&D' as schedule_name ,
(C+D-CD) as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'Total Combined AEW and NXT' as overlap_description  from first_15_schedules
union all

select dim_date_id,'Derived' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'A and B and C' as schedule_name ,
((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC) as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'Watched ALL WWE Total Combined SmackDown and Raw and NXT (includes AEW overlap)' as overlap_description  from first_15_schedules
union all

select dim_date_id,'Derived' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'B and C and D' as schedule_name ,
((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD) as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'Total Combined Raw and NXT and AEW (includes SmackDown overlap)' as overlap_description  from first_15_schedules
union all

select dim_date_id,'Derived' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'A and C and D' as schedule_name ,
((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD) as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'Total Combined SmackDown and NXT and AEW (includes Raw overlap)' as overlap_description  from first_15_schedules
union all

select dim_date_id,'Derived' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'A and B and D' as schedule_name ,
((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD) as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'Total Combined SmackDown and Raw and AEW (includes NXT overlap)' as overlap_description  from first_15_schedules
union all

select dim_date_id,'4-Way O/O/O/O/B Results' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'A and B and C and D' as schedule_name ,
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))) as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'Watched ALL Wrestling (SmackDown AND Raw AND NXT AND AEW)' as overlap_description  from first_15_schedules
union all

select dim_date_id,'4-Way O/O/O/O/B Results' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'A and B and C Only' as schedule_name ,
(((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))) as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'Watched ALL WWE ONLY (SmackDown AND Raw AND NXT ONLY, no AEW)' as overlap_description  from first_15_schedules
union all

select dim_date_id,'4-Way O/O/O/O/B Results' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'B and C and D Only' as schedule_name ,
(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))) as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'Watched Raw AND NXT AND AEW ONLY (no SmackDown)' as overlap_description  from first_15_schedules
union all

select dim_date_id,'4-Way O/O/O/O/B Results' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'C and D and A Only' as schedule_name ,
((((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD)))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))) as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'Watched NXT AND AEW AND SmackDown ONLY (no Raw)' as overlap_description  from first_15_schedules
union all

select dim_date_id,'4-Way O/O/O/O/B Results' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'A and B and D Only' as schedule_name ,
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))) as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'Watched SmackDown AND Raw AND AEW ONLY (no NXT)' as overlap_description  from first_15_schedules
union all

select dim_date_id,'4-Way O/O/O/O/B Results' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'A and B Only' as schedule_name ,
(A+B-AB)-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))) as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'Watched SmackDown AND Raw ONLY (no NXT or AEW)' as overlap_description  from first_15_schedules
union all

select dim_date_id,'4-Way O/O/O/O/B Results' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'B and C Only' as schedule_name ,
(B+C-BC)-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))) as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'Watched Raw AND NXT ONLY (no SmackDown or AEW)' as overlap_description  from first_15_schedules
union all

select dim_date_id,'4-Way O/O/O/O/B Results' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'C and D Only' as schedule_name ,
(C+D-CD)-((((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
(((((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD)))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))) as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'Watched NXT AND AEW ONLY (no Raw or SmackDown)' as overlap_description  from first_15_schedules
union all

select dim_date_id,'4-Way O/O/O/O/B Results' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'A and D Only' as schedule_name ,
(A+D-AD)-(((((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD)))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))) as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'Watched SmackDown AND AEW ONLY (no Raw or NXT)' as overlap_description  from first_15_schedules
union all

select dim_date_id,'4-Way O/O/O/O/B Results' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'B and D Only' as schedule_name ,
(B+D-BD)-((((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))) as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'Watched Raw AND AEW ONLY (no SmackDown or NXT)' as overlap_description  from first_15_schedules
union all

select dim_date_id,'4-Way O/O/O/O/B Results' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'A and C Only' as schedule_name ,
(A+C-CA)-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
(((((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD)))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))) as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'Watched SmackDown AND NXT ONLY (no Raw or AEW)' as overlap_description  from first_15_schedules
union all

select dim_date_id,'4-Way O/O/O/O/B Results' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'A Only' as schedule_name ,
A-(((A+B-AB)-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))+
((A+D-AD)-(((((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD)))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))+
((A+C-CA)-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
(((((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD)))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))+
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))+
(((((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD)))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))+
((((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))+
(((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))
) as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'Watched SmackDown ONLY (no overlap)' as overlap_description  from first_15_schedules
union all

select dim_date_id,'4-Way O/O/O/O/B Results' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'B Only' as schedule_name ,
B-(((A+B-AB)-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))+
((B+C-BC)-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))+
((B+D-BD)-((((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))+
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))+
((((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))+
((((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))+
(((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))) as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'Watched Raw ONLY (no overlap)' as overlap_description  from first_15_schedules
union all

select dim_date_id,'4-Way O/O/O/O/B Results' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'C Only' as schedule_name ,
C-(((B+C-BC)-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))+
((C+D-CD)-((((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
(((((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD)))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))+
((A+C-CA)-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
(((((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD)))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))+
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))+
((((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))+
(((((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD)))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))+
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD)))) as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'Watched NXT ONLY (no overlap)' as overlap_description  from first_15_schedules
union all

select dim_date_id,'4-Way O/O/O/O/B Results' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'D Only' as schedule_name ,
D-(((C+D-CD)-((((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
(((((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD)))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))+
((A+D-AD)-(((((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD)))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))+
((B+D-BD)-((((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))+
((((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))+
(((((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD)))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))+
((((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))+
(((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))) as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'Watched AEW ONLY (no ovelap)' as overlap_description  from first_15_schedules
union all

SELECT a.dim_date_id,
       'Straight Nielsen Run' as input_type,
       a.coverage_area,
       a.src_market_break,
       a.src_demographic_group,
       a.src_playback_period_cd,
       a.schedule_name,
       sum(a.aa_reach_proj000) as AA_Reac_Proj_000,
       cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(b.Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,
	   case when schedule_name like '%A | SmackDown (FOX)%' then 'Total Unique SmackDown'
			when schedule_name like '%B | Raw (USA)%' then 'Total Unique Raw'
			when schedule_name like '%C | NXT (USA)%' then 'Total Unique NXT'
			when schedule_name like '%D | AEW (TNT)%' then 'Total Unique AEW'
			
			when schedule_name='AB' then 'Total Combined SmackDown and/or Raw'
			
			when schedule_name='BC' then 'Total Combined Raw and/or NXT'
			
			when schedule_name='CA' then 'Total Combined NXT and/or SmackDown'
			
			when schedule_name='AD' then 'Total Combined SmackDown and/or AEW'
			
			when schedule_name='BD' then 'Total Combined Raw and/or AEW'
			
			when schedule_name='CD' then 'Total Combined NXT and/or AEW'
			
			when schedule_name='ABC' then 'Total Watched ANY WWE (Total Combined SmackDown and/or Raw and/or NXT)'
			
			when schedule_name='ABD' then 'Total Combined SmackDown and/or Raw and/or AEW'
			
			when schedule_name='BCD' then 'Total Combined Raw and/or NXT and/or AEW'
			
			when schedule_name='ACD' then 'Total Combined SmackDown and/or NXT and/or AEW'
			
			when schedule_name='ABCD' then 'Total Watched ANY Wrestling (SmackDown and/or Raw and/or NXT and/or AEW)'
			 end as Overlap_Description
FROM     fds_nl.fact_nl_weekly_overlap_4_way_oob a join first_15_schedules b on a.dim_date_id=b.dim_date_id
where a.etl_insert_rec_dttm  >  coalesce ((select max(etl_insert_rec_dttm) from "entdwdb"."fds_nl"."rpt_nl_weekly_overlap_derived_4_way_oob"), '1900-01-01 00:00:00')
--where a.dim_date_id='20200302'
GROUP BY 1,2,3,4,5,6,7,b.Max_AA_Reac_Proj_000)
select dim_date_id week_starting_date,
input_type,
coverage_area,
src_market_break market_break,
src_demographic_group demographic_group,
src_playback_period_cd playback_period_cd,
schedule_name program_combination,
aa_reac_proj_000 p2_total_unique_reach_proj,
p2_total_unique_reach_percent,
overlap_description,'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_4B' AS etl_batch_id,
'bi_dbt_user_prd'                                   AS etl_insert_user_id,
CURRENT_TIMESTAMP                                   AS etl_insert_rec_dttm,
NULL                                                AS etl_update_user_id,
CAST( NULL AS TIMESTAMP)                            AS etl_update_rec_dttm from total_schedules a