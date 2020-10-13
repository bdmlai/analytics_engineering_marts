/***********************************************************************************/
/*                    tier 3 model - rules based behaviors-final                   */
/* db:analytics_workspace schema:content                                           */
/* input tables: jw_t3_vc_clustering_sub_rules_ly                                  */
/*               jw_t3_vc_user_act_month                                           */
/* ouput tables: jw_t3_vc_clustering_final_ly                                      */
/***********************************************************************************/
{{
    config(
        materialized='table'
    )
}}

with 

base as (
    
    select * from {{ ref('jw_t3_vc_clustering_sub_rules_ly') }}
    
),

act_month as (
    
    select * from {{ ref('jw_t3_vc_user_act_month') }}
    
),

/* creates predominant behaviors */
model as(
    
    select 
    
        *,
        
        case                                           
            
            --less than one month activity
            when big_event_classification in ('less than one month activity') 
                then 'less than one month activity'
        
            --network fanatic        
            when (big_event_classification not in ('low big event viewership') 
                    and nxt_takeover_pct_watched_clean >= .1) -- big event on ;
                and (network_in_ring_classification not in ('low network in-ring viewership')) -- network in-ring on
                and (historic_in_ring_classification not in ('low historic in-ring viewership')) -- historic in-ring on
                and (original_classification not in ('low original viewership')) -- original on
              then 'wwe network fanatic' 
        
            --network enthusiast
            when (big_event_classification not in ('low big event viewership') 
                    and nxt_takeover_pct_watched_clean < .1) --ppv on ;                                   
                and (network_in_ring_classification not in ('low network in-ring viewership')) -- network in-ring on
                and (historic_in_ring_classification not in ('low historic in-ring viewership')) -- historic in-ring on 
                and (original_classification not in ('low original viewership')) -- original on
                then 'wwe network enthusiast'

            --current content fanatic
            when (big_event_classification not in ('low big event viewership') 
                    and nxt_takeover_pct_watched_clean >= .1) -- big event on ; 
                and (network_in_ring_classification not in ('low network in-ring viewership')) -- network in-ring on
                and (historic_in_ring_classification in ('low historic in-ring viewership')) -- historic in-ring off
                and (original_classification not in ('low original viewership')) -- original on
                then 'current content fanatic'
        
            --current content enthusiast
            when (big_event_classification not in ('low big event viewership') 
                    and nxt_takeover_pct_watched_clean < .1) --ppv on ;                                        
                and (network_in_ring_classification not in ('low network in-ring viewership')) -- network in-ring on
                and (historic_in_ring_classification in ('low historic in-ring viewership')) -- historic in-ring off 
                and (original_classification not in ('low original viewership')) -- original on
                then 'current content enthusiast'

            --now and then in-ring fanatic
            when (big_event_classification not in ('low big event viewership') 
                    and nxt_takeover_pct_watched_clean >= .1) -- big event on ; 
                and (network_in_ring_classification not in ('low network in-ring viewership')) -- network in-ring on
                and (historic_in_ring_classification not in ('low historic in-ring viewership')) -- historic in-ring on 
                and (original_classification in ('low original viewership')) -- original off
                then 'now and then in-ring fanatic' 
        
            --now and then in-ring enthusiast
            when (big_event_classification not in ('low big event viewership') 
                    and nxt_takeover_pct_watched_clean < .1) --ppv on ; 
                and (network_in_ring_classification not in ('low network in-ring viewership')) -- network in-ring on
                and (historic_in_ring_classification not in ('low historic in-ring viewership')) -- historic in-ring on 
                and (original_classification in ('low original viewership')) -- original off
                then 'now and then in-ring enthusiast' 

            --the original nostalgic fanatic
            when (big_event_classification not in ('low big event viewership') 
                    and nxt_takeover_pct_watched_clean >= .1) -- big event on ; 
                and (network_in_ring_classification in ('low network in-ring viewership')) -- network in-ring off
                and (historic_in_ring_classification not in ('low historic in-ring viewership')) -- historic in-ring on
                and (original_classification not in ('low original viewership')) -- original on
                then 'the original nostalgic fanatic'
        
            --the original nostalgic enthusiast'
            when (big_event_classification not in ('low big event viewership') 
                    and nxt_takeover_pct_watched_clean < .1) --ppv on ; 
                and (network_in_ring_classification in ('low network in-ring viewership')) -- network in-ring off
                and (historic_in_ring_classification not in ('low historic in-ring viewership')) -- historic in-ring on
                and (original_classification not in ('low original viewership')) -- original on
                then 'the original nostalgic ppv enthusiast'

            --current content in-ring fanatic
            when (big_event_classification not in ('low big event viewership') 
                    and nxt_takeover_pct_watched_clean >= .1) -- big event on ; 
                and (network_in_ring_classification not in ('low network in-ring viewership')) -- network in-ring on
                and (historic_in_ring_classification in ('low historic in-ring viewership')) -- historic in-ring off
                and (original_classification in ('low original viewership')) -- original off
                then 'current content in-ring fanatic' 
        
            --current content enthusiast
            when (big_event_classification not in ('low big event viewership') 
                    and nxt_takeover_pct_watched_clean < .1) --ppv on ; 
                and (network_in_ring_classification not in ('low network in-ring viewership')) -- network in-ring on
                and (historic_in_ring_classification in ('low historic in-ring viewership')) -- historic in-ring off
                and (original_classification in ('low original viewership')) -- original off
                then 'current content in-ring enthusiast'      

            --nostalgic fanatic
            when (big_event_classification not in ('low big event viewership') 
                    and nxt_takeover_pct_watched_clean >= .1) -- big event on ; 
                and (network_in_ring_classification in ('low network in-ring viewership')) -- network in-ring off
                and (historic_in_ring_classification not in ('low historic in-ring viewership')) -- historic in-ring on
                and (original_classification in ('low original viewership')) -- original off
                then 'nostalgic fanatic' 
        
            --nostalgic enthusiast
            when (big_event_classification not in ('low big event viewership') 
                    and nxt_takeover_pct_watched_clean < .1) --ppv on ; 
                and (network_in_ring_classification in ('low network in-ring viewership')) -- network in-ring off
                and (historic_in_ring_classification not in ('low historic in-ring viewership')) -- historic in-ring on
                and (original_classification in ('low original viewership')) -- original off
                then 'nostalgic ppv enthusiast' -- difference here for nostalgic fan

            --the original fanatic
            when (big_event_classification not in ('low big event viewership') 
                    and nxt_takeover_pct_watched_clean >= .1) -- big event on ; 
                and (network_in_ring_classification in ('low network in-ring viewership')) -- network in-ring off
                and (historic_in_ring_classification in ('low historic in-ring viewership')) -- historic in-ring off
                and (original_classification not in ('low original viewership')) -- original on
                then 'the original big event fanatic' 
        
            --the original enthusiast
            when (big_event_classification not in ('low big event viewership') 
                    and nxt_takeover_pct_watched_clean < .1) --ppv on ; 
                and (network_in_ring_classification in ('low network in-ring viewership')) -- network in-ring off
                and (historic_in_ring_classification in ('low historic in-ring viewership')) -- historic in-ring off
                and (original_classification not in ('low original viewership')) -- original on
                then 'the original ppv enthusiast'

            --big event fanatic
            when (big_event_classification not in ('low big event viewership') 
                    and nxt_takeover_pct_watched_clean >= .1) -- big event on ; 
                and (network_in_ring_classification in ('low network in-ring viewership')) -- network in-ring off
                and (historic_in_ring_classification in ('low historic in-ring viewership')) -- historic in-ring off
                and (original_classification in ('low original viewership')) -- original off
                then 'big event fanatic' 
                
            --ppv enthusiast
            when (big_event_classification not in ('low big event viewership') 
                    and nxt_takeover_pct_watched_clean < .1) --ppv on ; 
                and (network_in_ring_classification in ('low network in-ring viewership')) -- network in-ring off
                and (historic_in_ring_classification in ('low historic in-ring viewership')) -- historic in-ring off
                and (original_classification in ('low original viewership')) -- original off
                then 'ppv enthusiast'

            --good ole days fan
            when big_event_classification in ('low big event viewership') --big events and ppv off 
                and (network_in_ring_classification in ('low network in-ring viewership')) -- network in-ring off
                and (historic_in_ring_classification not in ('low historic in-ring viewership')) -- historic in-ring on 
                and (original_classification not in ('low original viewership')) -- original on (no difference for good ole days fans)
                then 'good ole days fan'
                
            when big_event_classification in ('low big event viewership') --big events and ppv off 
                and (network_in_ring_classification in ('low network in-ring viewership')) -- network in-ring off
                and (historic_in_ring_classification not in ('low historic in-ring viewership')) -- historic in-ring on 
                and (original_classification in ('low original viewership')) -- original off (no difference for good ole days fans)
                then 'good ole days fan'

            --low viewreship
            when big_event_classification in ('low big event viewership') --big events and ppv off 
                and (network_in_ring_classification in ('low network in-ring viewership')) -- network in-ring off
                and (historic_in_ring_classification in ('low historic in-ring viewership')) -- historic in-ring off
                and (original_classification in ('low original viewership')) -- original off
                then 'low viewership fan'
              
            else 'other' 
            
        end as predominant_classification

from al_t3_vc_clustering_sub_rules_ly

),

-- get all users who made payments but didn't watch any content on network
cc_users as (

    select  
    
        cast(src_fan_id as varchar) as src_fan_id,
        ly_act_mo_clean as act_months,                      -- change column name <time_window>_act_mo_clean
        'check your cc statement fan' as predominant_classification 

    from act_month
    
    where ly_act_mo_clean is not null                           -- change column name <time_window>_act_mo_clean
        and ly_act_mo_clean >= 0.5                                -- change column name <time_window>_act_mo_clean
        and src_fan_id not in (select distinct src_fan_id from model)
        
),
  
-- merge with the user consumption table
merged as (
    
    select 
    
        t1.*,
        t2.src_fan_id as src_fan_id_2,
        t2.act_months as act_months_2,
        t2.predominant_classification as predominant_classification_2
        
    from model as t1
    
    full join cc_users as t2
        on t1.src_fan_id = t2.src_fan_id
        
),

-- refine columns
merged_2 as (
    
    select 
    
        case
            when act_months is null then null
            else 'last year' 
        end as time_window,                       -- change <time window statement>
        case 
            when src_fan_id is not null 
                then src_fan_id
            else src_fan_id_2 
        end as src_fan_id,
        
        case 
            when act_months is not null 
                then act_months
            else act_months_2 
        end as act_months,
        
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
        all_viewership,
        network_tv_replays,
        toss_out,
        all_viewership_misc, -- including tv replays and toss out

        -- sub-behaviors
        big_event_classification,
        big_event_classification_v2,
        case
            when act_months < 1
                then 'less than one month activity'
            else network_in_ring_classification
        end as network_in_ring_classification,
        original_classification,
        historic_in_ring_classification,
        
        -- predominant behavior
        -- generate t1 ppv only fan as a subset of ppv enthusiasts
        case
        
            when big_event_classification in 
                (
                    'some t1, sample t2, weak to',
                    'some t1, weak t2, weak to',
                    't1 completer, weak t2, weak to',
                    't1 completer, sample t2, weak to'
                )
                and coalesce
                    (
                        predominant_classification,predominant_classification_2
                    ) = 'ppv enthusiast'
                then 'tier 1 ppv only fan'
        
            when act_months is null 
                then null
            
            else coalesce(predominant_classification,predominant_classification_2)
        
        end as predominant_classification
                

    from merged
    
),

-- here
-- reset time_window and predominant behaviors for users with null active months
merged_3 as (
    
    select 
    
        *,
        case when act_months is null then 1 else 0 end as toss_out_flag
    
    from merged_2
    
)

select * from merged_3
