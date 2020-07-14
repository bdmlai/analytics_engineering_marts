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

{{
  config({
		'schema': 'fds_nl',
		"pre-hook": ["delete from fds_nl.rpt_nl_weekly_overlap_derived_4_way_oob where etl_insert_rec_dttm > (select max(etl_insert_rec_dttm) from fds_nl.fact_nl_weekly_overlap_4_way_oob)"],
	     "materialized": 'incremental','tags': "Phase4B", "persist_docs": {'relation' : true, 'columns' : true}
  })
}}

{% set descript = [("AB","Total Combined SmackDown and/or Raw"),("BC","Total Combined Raw and/or NXT"),("CA","Total Combined NXT and/or SmackDown"),
("AD","Total Combined SmackDown and/or AEW"),("BD","Total Combined Raw and/or AEW"),("CD","Total Combined NXT and/or AEW"),
("ABC","Total Watched ANY WWE (Total Combined SmackDown and/or Raw and/or NXT)"),
("ABD","Total Combined SmackDown and/or Raw and/or AEW"),("BCD","Total Combined Raw and/or NXT and/or AEW"),("ACD","Total Combined SmackDown and/or NXT and/or AEW"),
("ABCD","Total Watched ANY Wrestling (SmackDown and/or Raw and/or NXT and/or AEW)")] %}

{% set schedule_formulas = [("(A+B-AB)","Both A&B","Derived","Total Combined SmackDown and Raw"),
("(A+C-CA)","Both A&C","Derived","Total Combined SmackDown and NXT"),
("(A+D-AD)","Both A&D","Derived","Total Combined SmackDown and AEW"),
("(B+D-BD)","Both B&D","Derived","Total Combined Raw and AEW"),
("(B+C-BC)","Both B&C","Derived","Total Combined Raw and NXT"),
("(C+D-CD)","Both C&D","Derived","Total Combined AEW and NXT"),
("((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC)","A and B and C","Derived","Watched ALL WWE Total Combined SmackDown and Raw and NXT (includes AEW overlap)"),
("((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD)","B and C and D","Derived","Total Combined Raw and NXT and AEW (includes SmackDown overlap)"),
("((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD)","A and C and D","Derived","Total Combined SmackDown and NXT and AEW (includes Raw overlap)"),
("((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD)","A and B and D","Derived","Total Combined SmackDown and Raw and AEW (includes NXT overlap)"),
("((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD)))","A and B and C and D","4-Way O/O/O/O/B Results","Watched ALL Wrestling (SmackDown AND Raw AND NXT AND AEW)"),
("(((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD)))","A and B and C Only","4-Way O/O/O/O/B Results","Watched ALL WWE ONLY (SmackDown AND Raw AND NXT ONLY, no AEW)"),
("(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD)))","B and C and D Only","4-Way O/O/O/O/B Results","Watched Raw AND NXT AND AEW ONLY (no SmackDown)"),
("((((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD)))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD)))","C and D and A Only","4-Way O/O/O/O/B Results","Watched NXT AND AEW AND SmackDown ONLY (no Raw)"),
("(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD)))","A and B and D Only","4-Way O/O/O/O/B Results","Watched SmackDown AND Raw AND AEW ONLY (no NXT)"),
("(A+B-AB)-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD)))","A and B Only","4-Way O/O/O/O/B Results","Watched SmackDown AND Raw ONLY (no NXT or AEW)"),
("(B+C-BC)-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD)))","B and C Only","4-Way O/O/O/O/B Results","Watched Raw AND NXT ONLY (no SmackDown or AEW)"),
("(C+D-CD)-((((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
(((((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD)))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD)))","C and D Only","4-Way O/O/O/O/B Results","Watched NXT AND AEW ONLY (no Raw or SmackDown)"),
("(A+D-AD)-(((((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD)))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD)))","A and D Only","4-Way O/O/O/O/B Results","Watched SmackDown AND AEW ONLY (no Raw or NXT)"),
("(B+D-BD)-((((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD)))","B and D Only","4-Way O/O/O/O/B Results","Watched Raw AND AEW ONLY (no SmackDown or NXT)"),
("(A+C-CA)-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
(((((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD)))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))-
((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD)))","A and C Only","4-Way O/O/O/O/B Results","Watched SmackDown AND NXT ONLY (no Raw or AEW)"),
("A-(((A+B-AB)-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
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
)","A Only","4-Way O/O/O/O/B Results","Watched SmackDown ONLY (no overlap)"),
("B-(((A+B-AB)-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
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
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD)))))","B Only","4-Way O/O/O/O/B Results","Watched Raw ONLY (no overlap)"),
("C-(((B+C-BC)-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
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
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD))))","C Only","4-Way O/O/O/O/B Results","Watched NXT ONLY (no overlap)"),
("D-(((C+D-CD)-((((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))-((((A+B-AB)+(A+C-CA)+(B+C-BC))-(A+B+C-ABC))+(((B+D-BD)+(C+D-CD)+(B+C-BC))-(B+C+D-BCD))+(((A+D-AD)+(C+D-CD)+(A+C-CA))-(A+C+D-ACD))+
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
(((A+D-AD)+(B+D-BD)+(A+B-AB))-(A+B+D-ABD))-(((A+B-AB)+(A+C-CA)+(A+D-AD)+(B+C-BC)+(B+D-BD)+(C+D-CD))-((A+B+C+D)-ABCD)))))","D Only","4-Way O/O/O/O/B Results","Watched AEW ONLY (no ovelap)")] %}

with first_15_schedules as 
(select dim_date_id,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,
sum(case when schedule_name like '%A | SmackDown (FOX)%' then aa_reach_proj000 end) A,
sum(case when schedule_name like '%B | Raw (USA)%' then aa_reach_proj000 end) B,
sum(case when schedule_name like '%C | NXT (USA)%' then aa_reach_proj000 end) C,
sum(case when schedule_name like '%D | AEW (TNT)%' then aa_reach_proj000 end) D,
{% for schedule_name,j in descript %}
sum(case when schedule_name = '{{schedule_name}}' then aa_reach_proj000 end) {{schedule_name}},
{% endfor %}
max(aa_reach_proj000) as Max_AA_Reac_Proj_000 
FROM   fds_nl.fact_nl_weekly_overlap_4_way_oob a
where a.etl_insert_rec_dttm  >  coalesce ((select max(etl_insert_rec_dttm) from {{this}}), '1900-01-01 00:00:00') 
--where dim_date_id='20200302'
GROUP BY 1,2,3,4,5),
 total_schedules as 
({% for schedule_formula,schedule_name,input_type,overlap_description in schedule_formulas %}
select dim_date_id,'{{input_type}}' as input_type,coverage_area,src_market_break,src_demographic_group,src_playback_period_cd,'{{schedule_name}}' as schedule_name ,
{{schedule_formula}} as AA_Reac_Proj_000,cast( Round(( cast(AA_Reac_Proj_000 as decimal(18,2))/nullif(cast(Max_AA_Reac_Proj_000 as decimal(18,2)),0))*100) as varchar)+'%' as P2_Total_Unique_Reach_Percent,'{{overlap_description}}' as overlap_description  from first_15_schedules
union all
{% endfor %}
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
			{% for schedule_nm,description in descript %}
			when schedule_name='{{schedule_nm}}' then '{{description}}'
			{% endfor %} end as Overlap_Description
FROM     fds_nl.fact_nl_weekly_overlap_4_way_oob a join first_15_schedules b on a.dim_date_id=b.dim_date_id
where a.etl_insert_rec_dttm  >  coalesce ((select max(etl_insert_rec_dttm) from {{this}}), '1900-01-01 00:00:00')
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