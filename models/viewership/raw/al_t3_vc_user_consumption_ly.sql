/***********************************************************************************/
/*             TIER 3 MODEL - USER CONSUMPTION(last year model)                    */
/* DB:ANALYTICS_WORKSPACE SCHEMA:CONTENT                                           */
/* INPUT TABLES: al_t3_vc_content_cluster                                          */
/*               al_t3_vc_user_act_month                                           */
/*               al_t3_vc_content_duration_ly                                      */
/* OUPUT TABLES: al_t3_vc_user_consumption_ly                                      */
/***********************************************************************************/
-- rolling up to user, content_class level
DROP TABLE IF EXISTS user_contclass;
CREATE TEMPORARY TABLE user_contclass AS(
SELECT src_fan_id,
       content_class,
       SUM(cluster_time) AS view_time
  FROM al_t3_vc_content_cluster                 -- table name      
 WHERE (short_term_flag = 1  or mid_term_flag = 1 )      -- CHANGE: using last year model for test, change to a variable if possible  
 --WHERE <flag_statement>                ----------- CHANGE FLAGS HERE
   --AND content_class <> 'Toss Out' 
   AND content_class IS NOT NULL
 GROUP BY src_fan_id,
       content_class);


-- normalizing view time and transposing data table
-- user,content_class level
DROP TABLE IF EXISTS user_contclass_t;
CREATE TEMPORARY TABLE user_contclass_t AS
SELECT src_fan_id,
       content_class, 

       -- Big Events (3)
       CASE WHEN content_class = 'Tier 1 PPV' THEN MAX(view_time) ELSE 0 END AS Tier_1_PPV,
       CASE WHEN content_class = 'Tier 2 PPV' THEN MAX(view_time) ELSE 0 END AS Tier_2_PPV,
       CASE WHEN content_class = 'NXT TakeOver' THEN MAX(view_time) ELSE 0 END AS NXT_TakeOver,

       -- Network In-Ring (5)
       CASE WHEN content_class = '205 Live' THEN MAX(view_time) ELSE 0 END AS _205_Live,
       CASE WHEN content_class = 'NXT' THEN MAX(view_time) ELSE 0 END AS NXT,
       CASE WHEN content_class = 'NXT UK' THEN MAX(view_time) ELSE 0 END AS NXT_UK,
       CASE WHEN content_class = 'In-Ring Tournament' THEN MAX(view_time) ELSE 0 END AS In_Ring_Tournament,
       CASE WHEN content_class = 'In-Ring Special' THEN MAX(view_time) ELSE 0 END AS In_Ring_Special,

       -- Original (3)
       CASE WHEN content_class = 'Historic Original' THEN MAX(view_time) ELSE 0 END AS historic_original,
       CASE WHEN content_class = 'Network Historic Original' THEN MAX(view_time) ELSE 0 END AS network_historic_original,
       CASE WHEN content_class = 'Network New Original' THEN MAX(view_time) ELSE 0 END AS network_new_original,

       --Historic In-Ring (10)
       CASE WHEN content_class = 'Attitude Era' THEN MAX(view_time) ELSE 0 END AS Attitude_Era,
       CASE WHEN content_class = 'ECW' THEN MAX(view_time) ELSE 0 END AS ECW,
       CASE WHEN content_class = 'Golden Era' THEN MAX(view_time) ELSE 0 END AS Golden_Era,
       CASE WHEN content_class = 'Historic Network In-Ring' THEN MAX(view_time) ELSE 0 END AS Historic_Network_In_Ring,
       CASE WHEN content_class = 'Indie Wrestling' THEN MAX(view_time) ELSE 0 END AS Indie_Wrestling,
       CASE WHEN content_class = 'New Era' THEN MAX(view_time) ELSE 0 END AS New_Era,
       CASE WHEN content_class = 'Reality Era' THEN MAX(view_time) ELSE 0 END AS Reality_Era,
       CASE WHEN content_class = 'Ruthless Aggression' THEN MAX(view_time) ELSE 0 END AS Ruthless_Aggression,
       CASE WHEN content_class = 'The New Generation Era' THEN MAX(view_time) ELSE 0 END AS The_New_Generation_Era,
       CASE WHEN content_class = 'WCW' THEN MAX(view_time) ELSE 0 END AS wcw,


       --Included for Completeness
       CASE WHEN content_class = 'Raw SD ME TV Replays' THEN MAX(view_time) ELSE 0 END AS Network_TV_Replays,
       CASE WHEN content_class = 'Toss Out' THEN MAX(view_time) ELSE 0 END AS Toss_Out
 FROM user_contclass
 GROUP BY src_fan_id,
       content_class;  




-- rolling up to user level
DROP TABLE IF EXISTS user_consumption;
CREATE TEMPORARY TABLE user_consumption AS(
SELECT  src_fan_id,
        
        --Big Events (3)
        max(tier_1_ppv) as tier_1_ppv,
        max(tier_2_ppv) as tier_2_ppv,
        max(nxt_takeover) as nxt_takeover,
        
        --Network In-Ring (5)
        max(_205_live) as _205_live,
        max(nxt) as nxt,
        max(nxt_uk) as nxt_uk,
        max(in_ring_tournament) as in_ring_tournament,
        max(in_ring_special) as in_ring_special,
          -- max(raw_sd_me_tv_replays) as raw_sd_me_tv_replays, -- not entirely sure how to account for this consumption
        
        --Original (3)
        max(historic_original) as historic_original,
        max(network_historic_original) as network_historic_original,
        max(network_new_original) as network_new_original,
        
        --Historic In-Ring   (10)  
        max(attitude_era) as attitude_era,
        max(ecw) as ecw,
        max(Golden_Era) as Golden_Era,
        max(historic_network_in_ring) as historic_network_in_ring,
        max(indie_wrestling) as indie_wrestling,
        max(New_Era) as New_Era,
        max(Reality_Era) as Reality_Era,
        max(Ruthless_Aggression) as Ruthless_Aggression,
        max(The_New_Generation_Era) as The_New_Generation_Era,
        max(wcw) as wcw,

        --Included for Completeness
        max(network_tv_replays) as network_tv_replays,
        max(toss_out) as toss_out

FROM user_contclass_t
GROUP BY src_fan_id);


-- adding aggregated consumption for each category
DROP TABLE IF EXISTS user_consumption_2;
create temporary table user_consumption_2 AS(
SELECT *,
       -- big events (3)
       tier_1_ppv + tier_2_ppv + nxt_takeover as big_event,
       -- network in-rings (5)
       _205_live + in_ring_tournament + nxt + nxt_uk + in_ring_special as network_in_ring, 
       -- network originals (3)
       historic_original + network_historic_original + network_new_original as original,   
       -- historic in-rings (10)
       attitude_era + ecw + golden_era + historic_network_in_ring + indie_wrestling 
       + new_era + reality_era + ruthless_aggression + the_new_generation_era + wcw as historic_in_ring 
FROM user_consumption);


-- HERE

-- addting ntiles and overall consumption
DROP TABLE IF EXISTS user_consumption_3;
create temporary table user_consumption_3 AS(
SELECT src_fan_id,

       -- Big Events
       big_event,
       tier_1_ppv,
       tier_2_ppv,
       nxt_takeover,
       
       --Network In-Ring
       network_in_ring,
       _205_live,
       in_ring_tournament,
       nxt,
       nxt_uk,
       in_ring_special,
       
       --Originals
       original,
       historic_original,
       network_historic_original,
       network_new_original,

       --Historic In-Ring
       historic_in_ring,
       --  ntile(20) over(order by historic_in_ring) as historic_in_ring_ntile,
       attitude_era,
       ecw,
       golden_era,
       historic_network_in_ring,
       indie_wrestling,
       new_era,
       reality_era,
       ruthless_aggression,
       the_new_generation_era,
       wcw,

      --All Viewership Combined
      big_event + network_in_ring + original + historic_in_ring as all_viewership,
      --  ntile(20) over(order by big_event + network_in_ring + original + historic_in_ring) as all_ntile
      
      -- for completeness (including toss out categories)
      network_tv_replays,
      toss_out,
      all_viewership + network_tv_replays + toss_out as all_viewership_misc
FROM user_consumption_2);


-- calculating average minutes per active month for each user
drop table if exists user_consumption_4;
create temporary table user_consumption_4 as 
select t1.*,
       t2.ly_act_mo_clean as act_months,
       coalesce(t1.big_event / (t2.ly_act_mo_clean*1.0),0) as big_event_monthly,
       coalesce(t1.network_in_ring / (t2.ly_act_mo_clean*1.0),0) as network_in_ring_monthly,
       coalesce(t1.original / (t2.ly_act_mo_clean*1.0),0) as original_monthly,
       coalesce( t1.historic_in_ring / (t2.ly_act_mo_clean*1.0),0) as historic_in_ring_monthly,
       coalesce(t1.all_viewership  / (t2.ly_act_mo_clean*1.0),0) as all_viewership_monthly,
       -- toss_out_flag: 1 if the user belongs to roger or company account, 
       -- i.e., anyone who has consumption over the period but there's no payment records
       case when t2.src_fan_id is null then 1 else 0 end as toss_out_flag
  from user_consumption_3 as t1
  left join (select * from al_t3_vc_user_act_month  
              where ly_act_mo_clean is not null) as t2  -- excludes any users with no payment record (could be apple users)
    on t1.src_fan_id = t2.src_fan_id;

-- RUN FROM HERE

DROP TABLE IF EXISTS user_consumption_5;
CREATE temporary TABLE user_consumption_5 AS(
SELECT a.*,
       b.time_window,
       -- big events
       b.big_event_dur,
         coalesce(a.big_event / nullif(b.big_event_dur,0),0) as big_event_pct_watched,
        (CASE WHEN big_event_pct_watched > 1 THEN 1
         ELSE big_event_pct_watched END) AS big_event_pct_watched_clean,
       b.tier_1_ppv_dur,
         coalesce(a.tier_1_ppv / nullif(tier_1_ppv_dur,0),0) as tier_1_pct_watched,
        (CASE WHEN tier_1_pct_watched > 1 THEN 1
         ELSE tier_1_pct_watched END) AS tier_1_pct_watched_clean,
       b.tier_2_ppv_dur,
         coalesce(a.tier_2_ppv / nullif(tier_2_ppv_dur,0),0) as tier_2_pct_watched,
        (CASE WHEN tier_2_pct_watched > 1 THEN 1
         ELSE tier_2_pct_watched END) AS tier_2_pct_watched_clean,
       b.nxt_takeover_dur,
         coalesce(a.nxt_takeover / nullif(nxt_takeover_dur,0),0) as nxt_takeover_pct_watched,
        (CASE WHEN nxt_takeover_pct_watched > 1 THEN 1
         ELSE nxt_takeover_pct_watched END) AS nxt_takeover_pct_watched_clean,
  
        -- in-ring
       b.network_in_ring_dur, 
         coalesce(a.network_in_ring / nullif(network_in_ring_dur,0),0) as network_in_ring_pct_watched,
        (CASE WHEN network_in_ring_pct_watched > 1 THEN 1
         ELSE network_in_ring_pct_watched END) AS network_in_ring_pct_watched_clean,
       b._205_live_dur,
         coalesce(a._205_live / nullif(_205_live_dur,0),0) as _205_live_pct_watched,
        (CASE WHEN _205_live_pct_watched > 1 THEN 1
         ELSE _205_live_pct_watched END) AS _205_live_pct_watched_clean,
       b.in_ring_tournament_dur,
         coalesce(a.in_ring_tournament / nullif(in_ring_tournament_dur,0),0) as in_ring_tournament_pct_watched,
        (CASE WHEN in_ring_tournament_pct_watched > 1 THEN 1
         ELSE in_ring_tournament_pct_watched END) AS in_ring_tournament_pct_watched_clean,
       b.nxt_dur,
         coalesce(a.nxt / nullif(nxt_dur,0),0) as nxt_pct_watched,
        (CASE WHEN nxt_pct_watched > 1 THEN 1
         ELSE nxt_pct_watched END) AS nxt_pct_watched_clean,
       b.nxt_uk_dur,
         coalesce(a.nxt_uk / nullif(nxt_uk_dur,0),0) as nxt_uk_pct_watched,
        (CASE WHEN nxt_uk_pct_watched > 1 THEN 1
         ELSE nxt_uk_pct_watched END) AS nxt_uk_pct_watched_clean,
       b.in_ring_special_dur,
           coalesce(a.in_ring_special / nullif(in_ring_special_dur,0),0) as in_ring_special_pct_watched,
        (CASE WHEN in_ring_special_pct_watched > 1 THEN 1
         ELSE in_ring_special_pct_watched END) AS in_ring_special_pct_watched_clean
  
  
       /*
       -- original
       b.original_dur,
       b.historic_original_dur,
       b.network_historic_original_dur,
       b.network_new_original_dur */
       
FROM user_consumption_4 a                 
    LEFT JOIN al_t3_vc_content_duration_ly  b             -- change table name here
        ON a.src_fan_id = b.src_fan_id);

-- select top 1000* from user_consumption_5 order by random();

-- output table
drop table if exists al_t3_vc_user_consumption_ly;        -- CHANGE TABLE NAME HERE
create table al_t3_vc_user_consumption_ly as              -- CHANGE TABLE NAME HERE
select * from user_consumption_5;

grant select on al_t3_vc_user_consumption_ly to public;   -- CHANGE TABLE NAME HERE

