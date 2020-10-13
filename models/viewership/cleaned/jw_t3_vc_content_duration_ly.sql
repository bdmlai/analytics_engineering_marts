/***********************************************************************************/
/*           TIER 3 MODEL - CONTENT DURATION CALCULATION(last year model)          */
/* DB:ANALYTICS_WORKSPACE SCHEMA:CONTENT                                           */
/* INPUT TABLES: jw_t3_vc_available_content                                          */
/* OUPUT TABLES: jw_t3_vc_content_duration_ly                                       */
/***********************************************************************************/
{{
    config(
        materialized='table'
    )
}}

with

base as (
    
    select * from {{ ref('jw_t3_vc_available_content') }}
    {{ limit_in_dev ('network_first_stream') }}
    
),

-- calculate content duration for premiering content available to each user during their active periods
-- only big events (ppv and takeovers) are included for now
-- should be null for all of the rest of the content class
duration_ly as (
    
    select
        
        src_fan_id,
        'last_year' as time_window,                   -- time window indicator
        -- big events
        sum(case when content_class in ('Tier 1 PPV', 'Tier 2 PPV', 'NXT TakeOver') then content_duration end) as big_event_dur, 
        sum(case when content_class in ('Tier 1 PPV') then content_duration end) as tier_1_ppv_dur,
        sum(case when content_class in ('Tier 2 PPV') then content_duration end) as tier_2_ppv_dur,
        sum(case when content_class in ('NXT TakeOver') then content_duration end) as nxt_takeover_dur,
        -- ntwk in-rings
        sum(case when content_class in ('205 Live', 'In-Ring Tournament', 'NXT', 'NXT UK','In-Ring Special') then content_duration end) as network_in_ring_dur, 
        sum(case when content_class in ('205 Live') then content_duration end) as _205_live_dur,
        sum(case when content_class in ('In-Ring Tournament') then content_duration end) as in_ring_tournament_dur,
        sum(case when content_class in ('NXT') then content_duration end) as nxt_dur,
        sum(case when content_class in ('NXT UK') then content_duration end) as nxt_uk_dur,
        sum(case when content_class in ('In-Ring Special') then content_duration end) as in_ring_special_dur,
        -- originals
        sum(case when content_class in ('Historic Original','Network Historic Original','Network New Original') then content_duration end) as original_dur,
        sum(case when content_class in ('Historic Original') then content_duration end) as historic_original_dur, 
        sum(case when content_class in ('Network Historic Original') then content_duration end) as network_historic_original_dur,
        sum(case when content_class in ('Network New Original') then content_duration end) as network_new_original_dur
       
    from base
    group by 1
    
)

select * from duration_ly