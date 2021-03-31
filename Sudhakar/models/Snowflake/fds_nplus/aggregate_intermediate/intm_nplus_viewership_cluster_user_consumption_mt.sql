/***********************************************************************************/
/*           5.  tier 3 model - user consumption(MT)                               */
/* db:analytics_workspace schema:content                                           */
/* input tables: intm_nplus_viewership_cluster_content_cluster                     */
/*               intm_nplus_viewership_cluster_user_act_month                      */
/*               intm_nplus_viewership_cluster_content_duration_mt                 */
/* ouput tables: intm_nplus_viewership_cluster_user_consumption_mt                 */
/***********************************************************************************/
{{
    config(
        materialized='table',
        tags=['viewership','viewership_model','mt'],
		schema='dt_stage'
    )
}}

with

base as (
    
    select * from {{ ref('intm_nplus_viewership_cluster_content_cluster') }}
    -- change: using last year model for test, change to a variable if possible  
    where mid_term_flag = 1     
    and content_class is not null
    
),

act_month as (
    
    select * from {{ ref('intm_nplus_viewership_cluster_user_act_month') }}
    
),

content_duration as (
    
    select * from {{ ref('intm_nplus_viewership_cluster_content_duration_mt') }}
    
),

-- rolling up to user, content_class level
user_contclass as (

    select 
    
        src_fan_id,
        content_class,
        sum(cluster_time) as view_time
  
    from base                 -- table name      
    
    
    
    group by 1,2
    
),

-- normalizing view time and transposing data table
-- user,content_class level
user_contclass_t as (
    
    select 
    
        src_fan_id,
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
    from user_contclass
    
    group by 1,2

),

-- rolling up to user level
user_consumption as (

    select  
        
        src_fan_id,
        
        --big events (3)
        max(tier_1_ppv) as tier_1_ppv,
        max(tier_2_ppv) as tier_2_ppv,
        max(nxt_takeover) as nxt_takeover,
        
        --network in-ring (5)
        max(_205_live) as _205_live,
        max(nxt) as nxt,
        max(nxt_uk) as nxt_uk,
        max(in_ring_tournament) as in_ring_tournament,
        max(in_ring_special) as in_ring_special,
          -- max(raw_sd_me_tv_replays) as raw_sd_me_tv_replays, -- not entirely sure how to account for this consumption
        
        --original (3)
        max(historic_original) as historic_original,
        max(network_historic_original) as network_historic_original,
        max(network_new_original) as network_new_original,
        
        --historic in-ring   (10)  
        max(attitude_era) as attitude_era,
        max(ecw) as ecw,
        max(golden_era) as golden_era,
        max(historic_network_in_ring) as historic_network_in_ring,
        max(indie_wrestling) as indie_wrestling,
        max(new_era) as new_era,
        max(reality_era) as reality_era,
        max(ruthless_aggression) as ruthless_aggression,
        max(the_new_generation_era) as the_new_generation_era,
        max(wcw) as wcw,

        --included for completeness
        max(network_tv_replays) as network_tv_replays,
        max(toss_out) as toss_out

    from user_contclass_t
    group by 1
    
),

-- adding aggregated consumption for each category
user_consumption_2 as (

    select 
    
        *,
       
        -- big events (3)
        tier_1_ppv + tier_2_ppv + nxt_takeover as big_event,
        
        -- network in-rings (5)
        _205_live + in_ring_tournament + nxt + nxt_uk + in_ring_special as network_in_ring, 
        
        -- network originals (3)
        historic_original + network_historic_original + network_new_original as original,   
        
        -- historic in-rings (10)
        attitude_era + ecw + golden_era + historic_network_in_ring + indie_wrestling 
            + new_era + reality_era + ruthless_aggression + the_new_generation_era + wcw 
            as historic_in_ring 
    
    from user_consumption
    
),

-- addting ntiles and overall consumption
user_consumption_3 as (

    select 
    
        src_fan_id,

        -- big events
        big_event,
        tier_1_ppv,
        tier_2_ppv,
        nxt_takeover,
       
        --network in-ring
        network_in_ring,
        _205_live,
        in_ring_tournament,
        nxt,
        nxt_uk,
        in_ring_special,
        
        --originals
        original,
        historic_original,
        network_historic_original,
        network_new_original,

        --historic in-ring
        historic_in_ring,
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

        --all viewership combined
        big_event + network_in_ring + original + historic_in_ring as all_viewership,

        -- for completeness (including toss out categories)
        network_tv_replays,
        toss_out,
        all_viewership + network_tv_replays + toss_out as all_viewership_misc

    from user_consumption_2
    
),

-- calculating average minutes per active month for each user
user_consumption_4 as (

    select 
    
        t1.*,
        t2.mt_act_mo_clean as act_months,
        coalesce(t1.big_event / (act_months*1.0),0) as big_event_monthly,
        coalesce(t1.network_in_ring / (act_months*1.0),0) as network_in_ring_monthly,
        coalesce(t1.original / (act_months*1.0),0) as original_monthly,
        coalesce( t1.historic_in_ring / (act_months*1.0),0) as historic_in_ring_monthly,
        coalesce(t1.all_viewership  / (act_months*1.0),0) as all_viewership_monthly,
        -- toss_out_flag: 1 if the user belongs to roger or company account, 
        -- i.e., anyone who has consumption over the period but there's no payment records
        case when t2.src_fan_id is null then 1 else 0 end as toss_out_flag
  
    from user_consumption_3 as t1
    
    left join act_month as t2
        on t1.src_fan_id = t2.src_fan_id
    
    where act_months is not null
    
),

user_consumption_5 as (

    select 
        
        a.*,
        b.time_window,
        -- big events
        b.big_event_dur,
        coalesce(a.big_event / nullif(b.big_event_dur,0),0) 
            as big_event_pct_watched,
        case 
            when big_event_pct_watched > 1 
                then 1
            else big_event_pct_watched 
        end as big_event_pct_watched_clean,
        
        b.tier_1_ppv_dur,
        coalesce(a.tier_1_ppv / nullif(tier_1_ppv_dur,0),0) 
            as tier_1_pct_watched,
        case 
            when tier_1_pct_watched > 1 
                then 1
            else tier_1_pct_watched 
        end as tier_1_pct_watched_clean,
        
        b.tier_2_ppv_dur,
        coalesce(a.tier_2_ppv / nullif(tier_2_ppv_dur,0),0) 
            as tier_2_pct_watched,
        case 
            when tier_2_pct_watched > 1 
                then 1
            else tier_2_pct_watched 
        end as tier_2_pct_watched_clean,
        
        b.nxt_takeover_dur,
        coalesce(a.nxt_takeover / nullif(nxt_takeover_dur,0),0) 
            as nxt_takeover_pct_watched,
        case 
            when nxt_takeover_pct_watched > 1 
                then 1
            else nxt_takeover_pct_watched 
        end as nxt_takeover_pct_watched_clean,
  
        -- in-ring
        b.network_in_ring_dur, 
        coalesce(a.network_in_ring / nullif(network_in_ring_dur,0),0) 
            as network_in_ring_pct_watched,
        case 
            when network_in_ring_pct_watched > 1 
                then 1
            else network_in_ring_pct_watched 
        end as network_in_ring_pct_watched_clean,
        
        b._205_live_dur,
        coalesce(a._205_live / nullif(_205_live_dur,0),0) 
            as _205_live_pct_watched,
        case 
            when _205_live_pct_watched > 1 
                then 1
            else _205_live_pct_watched 
        end as _205_live_pct_watched_clean,
        
        b.in_ring_tournament_dur,
        coalesce(a.in_ring_tournament / nullif(in_ring_tournament_dur,0),0) 
            as in_ring_tournament_pct_watched,
        case 
            when in_ring_tournament_pct_watched > 1 
                then 1
            else in_ring_tournament_pct_watched 
        end as in_ring_tournament_pct_watched_clean,
        
        b.nxt_dur,
        coalesce(a.nxt / nullif(nxt_dur,0),0) as nxt_pct_watched,
        case 
            when nxt_pct_watched > 1 
                then 1
            else nxt_pct_watched 
        end as nxt_pct_watched_clean,
        
        b.nxt_uk_dur,
        coalesce(a.nxt_uk / nullif(nxt_uk_dur,0),0) as nxt_uk_pct_watched,
        case 
            when nxt_uk_pct_watched > 1 
                then 1
            else nxt_uk_pct_watched 
        end as nxt_uk_pct_watched_clean,
        
        b.in_ring_special_dur,
        coalesce(a.in_ring_special / nullif(in_ring_special_dur,0),0) 
            as in_ring_special_pct_watched,
        case 
            when in_ring_special_pct_watched > 1 
                then 1
            else in_ring_special_pct_watched 
        end as in_ring_special_pct_watched_clean
  
        
    from user_consumption_4 a                 
    
    left join content_duration  b
        on a.src_fan_id = b.src_fan_id

)

select * from user_consumption_5