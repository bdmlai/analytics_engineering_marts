/***********************************************************************************/
/*             tier 3 model - user consumption(last year model)                    */
/* db:analytics_workspace schema:content                                           */
/* input tables: jw_t3_vc_content_cluster                                          */
/*               jw_t3_vc_user_act_month                                           */
/*               jw_t3_vc_content_duration_ly                                      */
/* ouput tables: jw_t3_vc_user_consumption_ly                                      */
/***********************************************************************************/
{{
    config(
        materialized='table',
        tag='viewership'
    )
}}

with

base as (
    
    select * from {{ ref('jw_t3_vc_content_cluster') }}
    
),

act_month as (
    
    select * from {{ ref('jw_t3_vc_user_act_month') }}
    
),

content_duration_ly as (
    
    select * from {{ ref('jw_t3_vc_content_duration_ly') }}
    
),

-- rolling up to user, content_class level
user_contclass as (

    select 
    
        src_fan_id,
        content_class,
        sum(cluster_time) as view_time
  
    from base                 -- table name      
    
    where (short_term_flag = 1  or mid_term_flag = 1 )      -- change: using last year model for test, change to a variable if possible  
    and content_class is not null
    
    group by 1,2
    
),

-- normalizing view time and transposing data table
-- user,content_class level
user_contclass_t as (
    
    select 
    
        src_fan_id,
        content_class, 

       -- big events (3)
       case when content_class = 'tier 1 ppv' then max(view_time) else 0 end as tier_1_ppv,
       case when content_class = 'tier 2 ppv' then max(view_time) else 0 end as tier_2_ppv,
       case when content_class = 'nxt takeover' then max(view_time) else 0 end as nxt_takeover,

       -- network in-ring (5)
       case when content_class = '205 live' then max(view_time) else 0 end as _205_live,
       case when content_class = 'nxt' then max(view_time) else 0 end as nxt,
       case when content_class = 'nxt uk' then max(view_time) else 0 end as nxt_uk,
       case when content_class = 'in-ring tournament' then max(view_time) else 0 end as in_ring_tournament,
       case when content_class = 'in-ring special' then max(view_time) else 0 end as in_ring_special,

       -- original (3)
       case when content_class = 'historic original' then max(view_time) else 0 end as historic_original,
       case when content_class = 'network historic original' then max(view_time) else 0 end as network_historic_original,
       case when content_class = 'network new original' then max(view_time) else 0 end as network_new_original,

       --historic in-ring (10)
       case when content_class = 'attitude era' then max(view_time) else 0 end as attitude_era,
       case when content_class = 'ecw' then max(view_time) else 0 end as ecw,
       case when content_class = 'golden era' then max(view_time) else 0 end as golden_era,
       case when content_class = 'historic network in-ring' then max(view_time) else 0 end as historic_network_in_ring,
       case when content_class = 'indie wrestling' then max(view_time) else 0 end as indie_wrestling,
       case when content_class = 'new era' then max(view_time) else 0 end as new_era,
       case when content_class = 'reality era' then max(view_time) else 0 end as reality_era,
       case when content_class = 'ruthless aggression' then max(view_time) else 0 end as ruthless_aggression,
       case when content_class = 'the new generation era' then max(view_time) else 0 end as the_new_generation_era,
       case when content_class = 'wcw' then max(view_time) else 0 end as wcw,

       --included for completeness
       case when content_class = 'raw sd me tv replays' then max(view_time) else 0 end as network_tv_replays,
       case when content_class = 'toss out' then max(view_time) else 0 end as toss_out
 
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

        --all viewership combined
        big_event + network_in_ring + original + historic_in_ring as all_viewership,
        --  ntile(20) over(order by big_event + network_in_ring + original + historic_in_ring) as all_ntile
        
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
    
    left join act_month as t2
        on t1.src_fan_id = t2.src_fan_id
    
    where ly_act_mo_clean is not null
    
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
    
    left join content_duration_ly  b
        on a.src_fan_id = b.src_fan_id

)

select * from user_consumption_5