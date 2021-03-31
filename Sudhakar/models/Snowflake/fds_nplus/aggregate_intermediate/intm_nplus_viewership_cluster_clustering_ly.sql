/***********************************************************************************/
/*                  7.  tier 3 model - rules based behaviors-final(ly)             */
/* db:analytics_workspace schema:content                                           */
/* input tables: intm_nplus_viewership_cluster_clustering_sub_rules_ly             */
/*               intm_nplus_viewership_cluster_user_act_month                      */
/* ouput tables: intm_nplus_viewership_cluster_clustering_ly                       */
/***********************************************************************************/
{{
    config(
        materialized='table',
        tags=['viewership','viewership_model','ly'],
		schema='dt_stage'
    )
}}

with 

base as (
    
    select * from {{ ref('intm_nplus_viewership_cluster_clustering_sub_rules_ly') }}
    
),

act_month as (
    
    select src_fan_id,
           ly_act_mo_clean as act_months
     from {{ ref('intm_nplus_viewership_cluster_user_act_month') }}
    where ly_act_mo_clean >= 0.5    
),

base_2 as (
    select t1.*,
           coalesce(t1.src_fan_id,t2.src_fan_id) as src_fan_id_v2,
           coalesce(t1.act_months,t2.act_months) as act_months_v2
      from base as t1
      full join act_month as t2
        on t1.src_fan_id = t2.src_fan_id
),
/* creates predominant behaviors */
model as(
    
    select 
    
        *,
        
        case   --Less Than One Month Activity
        when act_months_v2 is null then null    -- no classification for subs with no payment history
        WHEN act_months_v2 < 1 then 'Less Than One Month Activity'   -- new sign ups
        WHEN all_viewership_misc = 0 or all_viewership_misc is null  then 'Check Your CC Statement Fan'   -- has payment history but no viewership
        
        --WHEN big_event_classification IN ('Less Than One Month Activity') THEN 'Less Than One Month Activity'
        
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
        --Tier 1 PPV Only Fan
        WHEN (big_event_classification NOT IN ('Low Big Event Viewership') AND nxt_takeover_pct_watched_clean < .1) --PPV ON ; 
                AND (network_in_ring_classification IN ('Low Network In-Ring Viewership')) -- Network In-Ring OFF
                AND (historic_in_ring_classification IN ('Low Historic In-Ring Viewership')) -- Historic In-Ring OFF
                AND (original_classification IN ('Low Original Viewership')) -- Original OFF
                And big_event_classification in ('some t1, sample t2, weak to',
                                   'some t1, weak t2, weak to',
                                   't1 completer, weak t2, weak to',
                                   't1 completer, sample t2, weak to')
              THEN 'Tier 1 PPV Only Fan'      
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
              -- AND (original_classification NOT IN ('Low Original Viewership')) -- Original ON (No difference for Good Ole Days Fans)
              THEN 'Good Ole Days Fan'


        --Low viewreship
        WHEN big_event_classification IN ('Low Big Event Viewership') --BIG EVENTS AND PPV OFF 
                AND (network_in_ring_classification IN ('Low Network In-Ring Viewership')) -- Network In-Ring OFF
                AND (historic_in_ring_classification IN ('Low Historic In-Ring Viewership')) -- Historic In-Ring Off
                AND (original_classification IN ('Low Original Viewership')) -- Original OFF
              THEN 'Low Viewership Fan'
              
        ELSE 'Other' 
     END AS predominant_classification

from base_2

),

-- refine columns
merged_2 as (
    
    select 
                     
        src_fan_id_v2 as src_fan_id,
        act_months_v2 as act_months,
        
        -- big events
        big_event,
        big_event_monthly,
        big_event_pct_watched_clean,
        tier_1_ppv,
        tier_1_ppv_dur,
        tier_1_pct_watched_clean,
        tier_2_ppv,
        tier_2_ppv_dur,
        tier_2_pct_watched_clean,
        nxt_takeover,
        nxt_takeover_dur,
        nxt_takeover_pct_watched_clean,
        
        -- network in-rings
        network_in_ring,
        network_in_ring_monthly,
        network_in_ring_pct_watched_clean,
        _205_live,
        _205_live_dur,
        _205_live_pct_watched_clean,
        nxt,
        nxt_dur,
        nxt_pct_watched_clean,
        nxt_uk,
        nxt_uk_dur,
        nxt_uk_pct_watched_clean,
        in_ring_tournament,
        in_ring_tournament_dur,
        in_ring_tournament_pct_watched_clean,
        in_ring_special,
        in_ring_special_dur,
        in_ring_special_pct_watched_clean,

        -- originals
        original,
        original_monthly,
        historic_original,
        network_historic_original,
        network_new_original,
      
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
        all_viewership,
        network_tv_replays,
        toss_out,
        all_viewership_misc, -- including tv replays and toss out

        -- sub-behaviors
        big_event_classification,
        big_event_classification_v2,
        case
            when act_months < 1
                then 'Less Than One Month Activity'
            else network_in_ring_classification
        end as network_in_ring_classification,
        original_classification,
        historic_in_ring_classification,
        predominant_classification
                

    from model
    
),

-- here
-- reset time_window and predominant behaviors for users with null active months
merged_3 as (
    
    select 
        
        'last year' as model_name,
        ifnull(nullif('{{var("as_on_date")}}','None'),date_trunc(month,current_date)-1) as as_on_date,
        *
    from merged_2 where act_months is not null
    
)

select * from merged_3
