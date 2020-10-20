/***********************************************************************************/
/*                    TIER 3 MODEL - RULES BASED BEHAVIORS-Final                   */
/* DB:ANALYTICS_WORKSPACE SCHEMA:CONTENT                                           */
/* INPUT TABLES: al_t3_vc_clustering_sub_rules_ly                                  */
/* OUPUT TABLES: al_t3_vc_clustering_final_ly                                                                     */
/***********************************************************************************/

/* creates predominant behaviors */
DROP TABLE IF EXISTS model;
create temporary table model AS(
SELECT *,
 (CASE                                           
        --Less than One Month Activity
        WHEN big_event_classification IN ('Less than One Month Activity') THEN 'Less Than One Month Activity'
        
        --Network Fanatic        
        WHEN (big_event_classification NOT IN ('Low Big Event Viewership') AND nxt_takeover_pct_watched_clean >= .1) -- Big Event On ;
                AND (network_in_ring_classification NOT IN ('Low Network In-Ring Viewership')) -- Network In-Ring ON
                AND (historic_in_ring_classification NOT IN ('Low Historic In-Ring Viewership')) -- Historic In-Ring On
                AND (original_classification NOT IN ('Low Original Viewership')) -- Original ON
              THEN 'WWE Network Fanatic' 
        --Network Enthusiast
        WHEN (big_event_classification NOT IN ('Low Big Event Viewership') AND nxt_takeover_pct_watched_clean < .1) --PPV ON ;                                   
                AND (network_in_ring_classification NOT IN ('Low Network In-Ring Viewership')) -- Network In-Ring ON
                AND (historic_in_ring_classification NOT IN ('Low Historic In-Ring Viewership')) -- Historic In-Ring On 
                AND (original_classification NOT IN ('Low Original Viewership')) -- Original ON
              THEN 'WWE Network Enthusiast'

        --Current Content Fanatic
        WHEN (big_event_classification NOT IN ('Low Big Event Viewership') AND nxt_takeover_pct_watched_clean >= .1) -- Big Event On ; 
                AND (network_in_ring_classification NOT IN ('Low Network In-Ring Viewership')) -- Network In-Ring ON
                AND (historic_in_ring_classification IN ('Low Historic In-Ring Viewership')) -- Historic In-Ring OFF
                AND (original_classification NOT IN ('Low Original Viewership')) -- Original ON
              THEN 'Current Content Fanatic'
        --Current Content Enthusiast
        WHEN (big_event_classification NOT IN ('Low Big Event Viewership') AND nxt_takeover_pct_watched_clean < .1) --PPV ON ;                                        
                AND (network_in_ring_classification NOT IN ('Low Network In-Ring Viewership')) -- Network In-Ring ON
                AND (historic_in_ring_classification IN ('Low Historic In-Ring Viewership')) -- Historic In-Ring OFF 
                AND (original_classification NOT IN ('Low Original Viewership')) -- Original ON
              THEN 'Current Content Enthusiast'

        --Now and Then In-Ring Fanatic
        WHEN (big_event_classification NOT IN ('Low Big Event Viewership') AND nxt_takeover_pct_watched_clean >= .1) -- Big Event On ; 
                AND (network_in_ring_classification NOT IN ('Low Network In-Ring Viewership')) -- Network In-Ring ON
                AND (historic_in_ring_classification NOT IN ('Low Historic In-Ring Viewership')) -- Historic In-Ring On 
                AND (original_classification IN ('Low Original Viewership')) -- Original OFF
              THEN 'Now and Then In-Ring Fanatic' 
        --Now and Then In-Ring Enthusiast
        WHEN (big_event_classification NOT IN ('Low Big Event Viewership') AND nxt_takeover_pct_watched_clean < .1) --PPV ON ; 
                AND (network_in_ring_classification NOT IN ('Low Network In-Ring Viewership')) -- Network In-Ring ON
                AND (historic_in_ring_classification NOT IN ('Low Historic In-Ring Viewership')) -- Historic In-Ring On 
                AND (original_classification IN ('Low Original Viewership')) -- Original OFF
              THEN 'Now and Then In-Ring Enthusiast' 

         

        --The Original Nostalgic Fanatic
        WHEN (big_event_classification NOT IN ('Low Big Event Viewership') AND nxt_takeover_pct_watched_clean >= .1) -- Big Event On ; 
                AND (network_in_ring_classification IN ('Low Network In-Ring Viewership')) -- Network In-Ring OFF
                AND (historic_in_ring_classification NOT IN ('Low Historic In-Ring Viewership')) -- Historic In-Ring ON
                AND (original_classification NOT IN ('Low Original Viewership')) -- Original ON
              THEN 'The Original Nostalgic Fanatic'
        --The Original Nostalgic Enthusiast'
        WHEN (big_event_classification NOT IN ('Low Big Event Viewership') AND nxt_takeover_pct_watched_clean < .1) --PPV ON ; 
                AND (network_in_ring_classification IN ('Low Network In-Ring Viewership')) -- Network In-Ring OFF
                AND (historic_in_ring_classification NOT IN ('Low Historic In-Ring Viewership')) -- Historic In-Ring ON
                AND (original_classification NOT IN ('Low Original Viewership')) -- Original ON
              THEN 'The Original Nostalgic PPV Enthusiast'

        --Current Content In-Ring Fanatic
        WHEN (big_event_classification NOT IN ('Low Big Event Viewership') AND nxt_takeover_pct_watched_clean >= .1) -- Big Event On ; 
                AND (network_in_ring_classification NOT IN ('Low Network In-Ring Viewership')) -- Network In-Ring ON
                AND (historic_in_ring_classification IN ('Low Historic In-Ring Viewership')) -- Historic In-Ring OFF
                AND (original_classification IN ('Low Original Viewership')) -- Original OFF
              THEN 'Current Content In-Ring Fanatic' 
        --Current Content Enthusiast
        WHEN (big_event_classification NOT IN ('Low Big Event Viewership') AND nxt_takeover_pct_watched_clean < .1) --PPV ON ; 
                AND (network_in_ring_classification NOT IN ('Low Network In-Ring Viewership')) -- Network In-Ring ON
                AND (historic_in_ring_classification IN ('Low Historic In-Ring Viewership')) -- Historic In-Ring OFF
                AND (original_classification IN ('Low Original Viewership')) -- Original OFF
              THEN 'Current Content In-Ring Enthusiast'      

        --Nostalgic Fanatic
        WHEN (big_event_classification NOT IN ('Low Big Event Viewership') AND nxt_takeover_pct_watched_clean >= .1) -- Big Event On ; 
                AND (network_in_ring_classification IN ('Low Network In-Ring Viewership')) -- Network In-Ring OFF
                AND (historic_in_ring_classification NOT IN ('Low Historic In-Ring Viewership')) -- Historic In-Ring ON
                AND (original_classification IN ('Low Original Viewership')) -- Original OFF
              THEN 'Nostalgic Fanatic' 
        --Nostalgic Enthusiast
        WHEN (big_event_classification NOT IN ('Low Big Event Viewership') AND nxt_takeover_pct_watched_clean < .1) --PPV ON ; 
                AND (network_in_ring_classification IN ('Low Network In-Ring Viewership')) -- Network In-Ring OFF
                AND (historic_in_ring_classification NOT IN ('Low Historic In-Ring Viewership')) -- Historic In-Ring ON
                AND (original_classification IN ('Low Original Viewership')) -- Original OFF
              THEN 'Nostalgic PPV Enthusiast' -- Difference Here for Nostalgic Fan

        --The Original Fanatic
        WHEN (big_event_classification NOT IN ('Low Big Event Viewership') AND nxt_takeover_pct_watched_clean >= .1) -- Big Event On ; 
                AND (network_in_ring_classification IN ('Low Network In-Ring Viewership')) -- Network In-Ring OFF
                AND (historic_in_ring_classification IN ('Low Historic In-Ring Viewership')) -- Historic In-Ring OFF
                AND (original_classification NOT IN ('Low Original Viewership')) -- Original ON
              THEN 'The Original Big Event Fanatic' 
        --The Original Enthusiast
        WHEN (big_event_classification NOT IN ('Low Big Event Viewership') AND nxt_takeover_pct_watched_clean < .1) --PPV ON ; 
                AND (network_in_ring_classification IN ('Low Network In-Ring Viewership')) -- Network In-Ring OFF
                AND (historic_in_ring_classification IN ('Low Historic In-Ring Viewership')) -- Historic In-Ring OFF
                AND (original_classification NOT IN ('Low Original Viewership')) -- Original ON
              THEN 'The Original PPV Enthusiast'

        --Big Event Fanatic
        WHEN (big_event_classification NOT IN ('Low Big Event Viewership') AND nxt_takeover_pct_watched_clean >= .1) -- Big Event On ; 
                AND (network_in_ring_classification IN ('Low Network In-Ring Viewership')) -- Network In-Ring OFF
                AND (historic_in_ring_classification IN ('Low Historic In-Ring Viewership')) -- Historic In-Ring OFF
                AND (original_classification IN ('Low Original Viewership')) -- Original OFF
              THEN 'Big Event Fanatic' 
        --PPV Enthusiast
        WHEN (big_event_classification NOT IN ('Low Big Event Viewership') AND nxt_takeover_pct_watched_clean < .1) --PPV ON ; 
                AND (network_in_ring_classification IN ('Low Network In-Ring Viewership')) -- Network In-Ring OFF
                AND (historic_in_ring_classification IN ('Low Historic In-Ring Viewership')) -- Historic In-Ring OFF
                AND (original_classification IN ('Low Original Viewership')) -- Original OFF
              THEN 'PPV Enthusiast'

        --Good Ole Days Fan
        WHEN big_event_classification IN ('Low Big Event Viewership') --BIG EVENTS AND PPV OFF 
                AND (network_in_ring_classification IN ('Low Network In-Ring Viewership')) -- Network In-Ring OFF
                AND (historic_in_ring_classification NOT IN ('Low Historic In-Ring Viewership')) -- Historic In-Ring On 
                AND (original_classification NOT IN ('Low Original Viewership')) -- Original ON (No difference for Good Ole Days Fans)
              THEN 'Good Ole Days Fan'
        WHEN big_event_classification IN ('Low Big Event Viewership') --BIG EVENTS AND PPV OFF 
                AND (network_in_ring_classification IN ('Low Network In-Ring Viewership')) -- Network In-Ring OFF
                AND (historic_in_ring_classification NOT IN ('Low Historic In-Ring Viewership')) -- Historic In-Ring On 
                AND (original_classification IN ('Low Original Viewership')) -- Original OFF (No difference for Good Ole Days Fans)
              THEN 'Good Ole Days Fan'

        --Low viewreship
        WHEN big_event_classification IN ('Low Big Event Viewership') --BIG EVENTS AND PPV OFF 
                AND (network_in_ring_classification IN ('Low Network In-Ring Viewership')) -- Network In-Ring OFF
                AND (historic_in_ring_classification IN ('Low Historic In-Ring Viewership')) -- Historic In-Ring Off
                AND (original_classification IN ('Low Original Viewership')) -- Original OFF
              THEN 'Low Viewership Fan'
              
     ELSE 'Other' END) AS predominant_classification
FROM al_t3_vc_clustering_sub_rules_ly);


-- get all users who made payments but didn't watch any content on Network
drop table if exists cc_users;
create temporary table cc_users as 
select  cast(src_fan_id as varchar) as src_fan_id,
        ly_act_mo_clean as act_months,                      -- change column name <time_window>_act_mo_clean
        'Check Your CC Statement Fan' as predominant_classification 
from al_t3_vc_user_act_month
where ly_act_mo_clean is not null                           -- change column name <time_window>_act_mo_clean
  and ly_act_mo_clean >= 0.5                                -- change column name <time_window>_act_mo_clean
  and src_fan_id not in (select distinct src_fan_id from model);
  
-- merge with the user consumption table
drop table if exists merged;
create temporary table merged as
select t1.*,
       t2.src_fan_id as src_fan_id_2,
       t2.act_months as act_months_2,
       t2.predominant_classification as predominant_classification_2
from model as t1
full join cc_users as t2
on t1.src_fan_id = t2.src_fan_id;

-- refine columns
drop table if exists merged_2;
create temporary table merged_2 as
select 'last year' as time_window,                       -- change <time window statement>
        case when src_fan_id is not null then src_fan_id
          else src_fan_id_2 end as src_fan_id,
        case when act_months is not null then act_months
          else act_months_2 end as act_months,
        -- big events
        big_event,
        --big_event_dur,
        big_event_monthly,
        --big_event_pct_watched,
        big_event_pct_watched_clean,
        tier_1_ppv,
        tier_1_ppv_dur,
        --tier_1_pct_watched,
        tier_1_pct_watched_clean,
        tier_2_ppv,
        tier_2_ppv_dur,
        --tier_2_pct_watched,
        tier_2_pct_watched_clean,
        nxt_takeover,
        nxt_takeover_dur,
        --nxt_takeover_pct_watched,
        nxt_takeover_pct_watched_clean,
        
        -- network in-rings
        network_in_ring,
        --network_in_ring_dur,
        network_in_ring_monthly,
        --network_in_ring_pct_watched,
        network_in_ring_pct_watched_clean,
        _205_live,
        _205_live_dur,
        --_205_live_pct_watched,
        _205_live_pct_watched_clean,
        nxt,
        nxt_dur,
        --nxt_pct_watched,
        nxt_pct_watched_clean,
        nxt_uk,
        nxt_uk_dur,
        --nxt_uk_pct_watched,
        nxt_uk_pct_watched_clean,
        in_ring_tournament,
        in_ring_tournament_dur,
        --in_ring_tournament_pct_watched,
        in_ring_tournament_pct_watched_clean,
        in_ring_special,
        in_ring_special_dur,
        --in_ring_special_pct_watched,
        in_ring_special_pct_watched_clean,

        -- originals
        original,
        --original_dur,
        original_monthly,
        --original_pct_watched,
        --original_pct_watched_clean,
        historic_original,
        --historic_original_pct_watched,
        --historic_original_pct_watched_clean,
        network_historic_original,
        --network_historic_original_pct_watched,
        --network_historic_original_pct_watched_clean,
        network_new_original,
        --network_new_original_pct_watched,
       -- network_new_original_pct_watched_clean,
        

        -- historic in-ring
        historic_in_ring,
        historic_in_ring_monthly,
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
        
        -- all viewership
        ALL_VIEWERSHIP,
        NETWORK_TV_REPLAYS,
        TOSS_OUT,
        ALL_VIEWERSHIP_MISC, -- including tv replays and toss out

        -- sub-behaviors
        big_event_classification,
        big_event_classification_v2,
        network_in_ring_classification,
        original_classification,
        historic_in_ring_classification,
        
        -- predominant behavior
        case when predominant_classification is not null then predominant_classification
          else predominant_classification_2 end as predominant_classification
  from merged;


-- generate T1 PPV Only Fan as a subset of PPV Enthusiasts
update merged_2
set predominant_classification = 'Tier 1 PPV Only Fan'
where big_event_classification in ('some t1, sample t2, weak to',
                                   'some t1, weak t2, weak to',
                                   't1 completer, weak t2, weak to',
                                   't1 completer, sample t2, weak to')
and predominant_classification = 'PPV Enthusiast';

-- here
-- reset time_window and predominant behaviors for users with null active months
update merged_2 set time_window = null where act_months is null;
update merged_2 set predominant_classification = null where act_months is null; 
update merged_2 set network_in_ring_classification = 'Less than One Month Activity' where act_months < 1;


drop table if exists merged_3;
create temporary table merged_3 as
select *,
       case when act_months is null then 1 else 0 end as toss_out_flag
from merged_2;   

drop table if exists al_t3_vc_clustering_final_ly ;
create table al_t3_vc_clustering_final_ly  as select * from merged_3;
grant select on al_t3_vc_clustering_final_ly to public;

select top 1000* from al_t3_vc_clustering_final_ly order by random();

select predominant_classification, count(distinct src_fan_id) as no_users
  from al_t3_vc_clustering_final_ly
 group by 1 order by 2 desc;
