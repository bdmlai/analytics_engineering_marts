/***********************************************************************************/
/*                  1. tier 3 model - content cluster base                         */
/*                    db:analytics_workspace schema:content                        */
/* input tables: cdm.dim_content_classification_title                              */
/*               fds_nplus.aggr_daily_postshow_rankings                            */
/*               fds_nplus.fact_daily_content_viewership                           */
/* output table: intm_nplus_viewership_cluster_content_cluster                                          */
/***********************************************************************************/
/*-----------------------------------------------------------*/
/*                preparing video meta deta                  */
/*-----------------------------------------------------------*/
{{
    config(
        materialized='table',
        tags=['viewership','viewership_base'],
		schema='dt_stage'
    )
}}

with 

base as (
    
    select * from {{ source('cdm_prod','dim_content_classification_title') }}
    
),

daily_postshow_rankings as (
    
    select * from {{ source('fds_nplus_prod','aggr_daily_postshow_rankings') }}
    
),

daily_content_viewership as (
    
    select * from {{ source('fds_nplus_prod', 'fact_daily_content_viewership') }}
    where stream_start_dttm between '2015-01-01' and ifnull(nullif('{{var("as_on_date")}}','None'),date_trunc(month,current_date)-1)
    
),

rankings_renamed as (
    
    select distinct 
    
        production_id, 
        post_rank_flag,
        encore,
        start_est_dttm as start_time
        
    from daily_postshow_rankings
    where encore <> 'x'
    
),

-- meta data for all video content
base_renamed as (

    select 
        
        production_id,
        premiere_date as any_network_first_stream, 
        debut_date as first_stream,
        content_duration/60.0 as content_duration,
        class_nm as class_name,
        category_nm as category_name,
        event_style_nm as event_style_name,
        series_nm as series_name,
        series_group,
        episode_nm as content_title,
        ppv_brand_name,
        franchise_name,
        
        case 
            when premiere_date < '1964-01-01' then 'Capitol Era'
            when premiere_date between '1964-01-01' and '1977-04-29' then 'MSG Era'
            when premiere_date between '1977-04-30' and '1984-01-22' then 'Titan Era'
            when premiere_date between '1984-01-23' and '1992-10-11' then 'Golden Era'
            when premiere_date between '1992-10-12' and '1997-12-14' then 'The New Generation Era'
            when premiere_date between '1997-12-15' and '2002-01-23' then 'Attitude Era'
            when premiere_date between '2002-01-24' and '2008-07-19' then 'Ruthless Aggression'
            when premiere_date between '2008-07-20' and '2011-06-26' then 'Attitude Adjustment Era'
            when premiere_date between '2011-06-27' and '2016-04-03' then 'Reality Era'
            when premiere_date >= '2016-04-04' then 'New Era'
            else 'Unknown' 
        end as wwe_era
  
    from base
  
),

rankings_joined as (  

    select 
    
        base_renamed.*,
        rankings_renamed.post_rank_flag,
        rankings_renamed.start_time
    
    from base_renamed
    left join rankings_renamed
        on base_renamed.production_id = rankings_renamed.production_id

),

-- dedupe at production id level
deduped as (

    select 
    
        *,
        row_number() over (partition by production_id order by production_id desc) 
            as duplicate_flag
    
    from rankings_joined
        
),

reclassified as (
    
    select 
    
        *,
       case when franchise_name is null then 'WWE' else franchise_name end as clean_franchise,
       case
        /* Originals */
             -- toss out hall of fames, shoulder, specials
        WHEN class_name = 'Original' AND lower(content_title) like '%hall of fame%' THEN 'Toss Out'
        WHEN lower(series_group) like '%hall of fame%' THEN 'Toss Out'
        WHEN class_name = 'Original' AND lower(category_name) in ('shoulder (original)','special') THEN 'Toss Out'
        WHEN class_name = 'Original' AND lower(category_name) NOT IN ('shoulder (original)','special') THEN category_name
        
        /* other toss outs (test streams, etc) */
        WHEN content_title IN('Test','This is NXT 2017','WWE Draft Center Live') THEN 'Toss Out'
        WHEN class_name IS NULL THEN 'Toss Out'
        WHEN class_name = 'In-Ring' AND category_name = 'Shoulder' AND (event_style_name = 'Episodic' OR event_style_name IS NULL) THEN 'Toss Out'
        WHEN class_name = 'In-Ring' AND category_name = 'Highlights' THEN 'Toss Out'
        WHEN class_name = 'In-Ring' AND category_name IN ('Collections','Promos') THEN 'Toss Out'
        
        /*  PPVs and Kickoff Shows */
        WHEN class_name = 'In-Ring' AND category_name = 'Shoulder' AND event_style_name = 'PPV'
                                    AND lower(series_group) in ('wwe ppv kickoff show') AND LOWER(content_title) NOT LIKE '%fallout%' 
                                    AND (lower(content_title) like '%wrestlemania%'
                                         or lower(content_title) like '%royal rumble%'
                                         or lower(content_title) like '%summerslam%'
                                         or lower(content_title) like '%survivor series%')
                                    THEN 'Tier 1 PPV'
        WHEN class_name = 'In-Ring' AND category_name = 'Shoulder' AND event_style_name = 'PPV' 
                                    AND lower(series_group) in ('wwe ppv kickoff show') AND LOWER(content_title) NOT LIKE '%fallout%'
                                    AND (lower(content_title) not like '%wrestlemania%'
                                          and lower(content_title) not like '%royal rumble%'
                                          and lower(content_title) not like '%summerslam%'
                                          and lower(content_title) not like '%survivor series%')
                                     THEN 'Tier 2 PPV'
        WHEN class_name = 'In-Ring' AND category_name = 'Event'  AND event_style_name = 'PPV' 
                                    AND lower(series_group) in ('wwe ppv') AND LOWER(content_title) NOT LIKE '%fallout%' 
                                    AND (lower(content_title) like '%wrestlemania%'
                                         or lower(content_title) like '%royal rumble%'
                                         or lower(content_title) like '%summerslam%'
                                         or lower(content_title) like '%survivor series%')
                                    THEN 'Tier 1 PPV'
        WHEN class_name = 'In-Ring' AND category_name = 'Event'    AND event_style_name = 'PPV' 
                                    AND lower(series_group) in ('wwe ppv') 
                                    AND LOWER(content_title) NOT LIKE '%fallout%'
                                    AND (lower(content_title) not like '%wrestlemania%'
                                         and lower(content_title) not like '%royal rumble%'
                                         and lower(content_title) not like '%summerslam%'
                                          and lower(content_title) not like '%survivor series%')
                                     THEN 'Tier 2 PPV'
      
        /* NXT TakeOver */
        WHEN class_name = 'In-Ring' AND category_name = 'Shoulder' AND event_style_name = 'Special' AND lower(series_group)='nxt takeover - pre-show' then 'NXT TakeOver'
        WHEN class_name = 'In-Ring' AND category_name = 'Shoulder' AND event_style_name = 'Special' AND lower(series_group)='nxt uk takeover - pre-show' THEN 'NXT TakeOver'
        WHEN class_name = 'In-Ring' AND category_name = 'Event' AND event_style_name = 'Special' AND lower(series_group)= 'nxt takeover' THEN 'NXT TakeOver'
        WHEN class_name = 'In-Ring' AND category_name = 'Event' AND event_style_name = 'Special' AND lower(series_group)= 'nxt uk takeover' THEN 'NXT TakeOver'
      
        /* In-Rings */
        WHEN class_name = 'In-Ring' AND category_name = 'Event' AND event_style_name = 'Episodic' AND lower(series_group) = '205 live' THEN '205 Live'
        WHEN class_name = 'In-Ring' AND category_name = 'Event' AND event_style_name = 'Episodic' AND lower(series_group) = 'nxt' THEN 'NXT'
        WHEN class_name = 'In-Ring' AND category_name = 'Event' AND event_style_name = 'Episodic' AND lower(series_group) = 'nxt uk' THEN 'NXT UK'
        WHEN class_name = 'In-Ring' AND category_name = 'Event' AND event_style_name = 'Tournament' THEN 'In-Ring Tournament'
        
        /* Special In-ring Events */ 
        WHEN (class_name = 'In-Ring' and category_name = 'Event' and event_style_name = 'Special' and series_name = 'Special Event'
               and any_network_first_stream = first_stream)
             or production_id in ('star2018_ntwk','evolve10th_ntwk') THEN 'In-Ring Special'

        /* Others */  
        WHEN lower(series_group) IN('raw','smackdown','wwe main event') THEN 'Raw SD ME TV Replays'
        WHEN lower(series_group) IN('theme weeks','raw shoulder old') THEN 'Toss Out'
        WHEN lower(series_group) IN ('wwe raw talk','wwe talking smack', 'raw talk','talking smack') THEN 'Toss Out'
        
       ELSE 'Unclassified' END AS first_class
  
    from deduped
    where duplicate_flag = 1
  
),  

/*-----------------------------------------------------------*/
/*                preparing stream base table                 */
/*-----------------------------------------------------------*/
-- add video meta data to network viewership data at stream level

classifications_joined as (

    select 
        
        daily_content_viewership.subs_tier,
        daily_content_viewership.content_tier,
        daily_content_viewership.stream_tier,
        daily_content_viewership.stream_id,
        daily_content_viewership.src_fan_id,
        daily_content_viewership.stream_start_dttm as min_time,
        daily_content_viewership.play_time,
        daily_content_viewership.time_spent,
        to_date(daily_content_viewership.stream_start_dttm) as view_day,
        daily_content_viewership.stream_type_cd as stream_type,
        daily_content_viewership.production_id as production_id_vt,
        reclassified.*
        
    from daily_content_viewership
    
    left join reclassified
        on daily_content_viewership.production_id = reclassified.production_id
       and subs_tier = 3
  
),

-- further classifications  
additional_classifications as (

    select 
        
        *,
        (CASE  WHEN first_stream is NULL then 'Toss Out'
              WHEN first_class = 'Tier 1 PPV' THEN 'Tier 1 PPV'
              WHEN first_class = 'Tier 2 PPV' THEN 'Tier 2 PPV'
              WHEN first_class = 'NXT TakeOver' THEN 'NXT TakeOver'
              WHEN first_class IN ('205 Live','NXT','In-Ring Tournament','NXT UK','In-Ring Special') THEN 'Network In-Ring'
              WHEN first_class = 'Toss Out' THEN  'Toss Out'
              WHEN first_class IN ('Animation','Best of','Documentary','Event','Lifestyle','News','Reality','Scripted Series','Talk Show') THEN 'Original'
              WHEN first_class = 'Unclassified' THEN 'Historic In-Ring'
          ELSE 'Unclassified' END) AS clustering_1,
        
        (CASE WHEN first_class = 'Unclassified' AND clean_franchise = 'WWE' THEN wwe_era
              WHEN first_class = 'Unclassified' AND clean_franchise <> 'WWE' THEN clean_franchise
              WHEN first_class <> 'Unclassified' THEN ''
          ELSE '' END) AS historic_in_ring_class 
    
    from classifications_joined

),

-- clean up historic in-ring and network originals classification
cleaned as (

    select 
        *,
         -- For In-Ring and Big Events, if the show was viewed after 90 days from debut, then reclassify them into historic in-ring
      (CASE WHEN first_class IN ('Tier 1 PPV','Tier 2 PPV') AND datediff(d,any_network_first_stream,min_time) > 90 THEN wwe_era 
        WHEN first_class = 'NXT TakeOver' AND datediff(d,any_network_first_stream,min_time) > 90 THEN 'Historic NXT TakeOver' 
        WHEN first_class = 'NXT' AND datediff(d,any_network_first_stream,min_time) > 90 THEN 'Historic NXT' 
        WHEN first_class = 'NXT UK' AND datediff(d,any_network_first_stream,min_time) > 90 THEN 'Historic NXT UK' 
        WHEN first_class = '205 Live' AND datediff(d,any_network_first_stream,min_time) > 90 THEN 'Historic 205 Live' 
        WHEN first_class = 'In-Ring Tournament' AND datediff(d,any_network_first_stream,min_time) > 90 THEN 'Historic In-Ring Tournament' 
        WHEN first_class = 'In-Ring Special' AND datediff(d,any_network_first_stream,min_time) > 90 THEN 'Historic In-Ring Special'  
        WHEN first_class = 'Raw SD ME TV Replays' AND datediff(d,any_network_first_stream,min_time) > 90 THEN wwe_era 
        
        -- Sept-2019 Reclassify Originals based on premiere date and view day
          -- premiering on a platform other than Network first
        WHEN clustering_1 = 'Original' AND any_network_first_stream < to_date(first_stream) - 7 THEN 'Historic Original'
          -- viewed after 90 days of its debut date
        WHEN clustering_1 = 'Original' AND view_day > to_date(first_stream) + 90 then 'Network Historic Original'
          -- viewed within 90 days of its debut date
        WHEN clustering_1 = 'Original' AND view_day <= to_date(first_stream) + 90 then 'Network New Original'

        WHEN clustering_1 = 'Historic In-Ring' THEN historic_in_ring_class
      ELSE first_class END) AS content_class_0,

        -- toss out some linear consumption, keeps big events and in-rings
        (CASE WHEN first_class IN ('Tier 1 PPV','Tier 2 PPV','NXT TakeOver','205 Live','NXT','NXT UK','In-Ring Tournament','In-Ring Special') 
                        AND stream_type = 'linear' THEN play_time
                WHEN post_rank_flag IN ('Post-Raw','Post-SmackDown','Post-NXT') AND stream_type = 'linear' AND to_date(min_time) = to_date(start_time) THEN play_time
                WHEN stream_type = 'vod' THEN play_time
            ELSE 0 END) AS cluster_time

from additional_classifications

),

-- add flags to indicate the which stream level would be included into models with different timeframes
with_flags as (
    
    select 
        *,
         -- regroup historic in-ring
       case when content_class_0 in ('Historic 205 Live', 'Historic NXT','Historic NXT TakeOver',
                                     'Historic In-Ring Tournament','Historic NXT UK','Historic In-Ring Special') then 'Historic Network In-Ring'
            when content_class_0 in ('AWA','GWF','JCP', 'MSW','SMW','WWWF','WCCW','STAMPEDE',
                                     'EVOLVE','ICW','PROGRESS','wXw') then 'Indie Wrestling'
            when content_class_0 in ('Golden Era','Titan Era','Capitol Era','MSG Era') then 'Golden Era'
            when content_class_0 in ('Attitude Adjustment Era','Reality Era') then 'Reality Era'
          else content_class_0 end as content_class,
      -- regroup content_class into high level categories
       case when content_class in ('Tier 1 PPV','Tier 2 PPV','NXT TakeOver') then 'Big Event'
            when clustering_1 = 'Original' then 'Original'
            when content_class in ('205 Live','NXT','NXT UK','In-Ring Special','In-Ring Tournament') then 'Network In-Ring'
            when content_class in ('Attitude Era','ECW','Golden Era','Historic Network In-Ring','Indie Wrestling',
                                   'New Era','Reality Era','Ruthless Aggression',
                                   'The New Generation Era','WCW') then 'Historic In-Ring'
            else 'Toss Out' end as content_category,
         
        to_date(to_date(to_char(ifnull(nullif('{{var("as_on_date")}}','None'),date_trunc(month,current_date)-1)),'yyyy-mm-dd')) as as_on_date,
        
        -- add flags to indicate the timeframes
        dateadd(mon,-3,as_on_date) as prev_3_month,
        dateadd(mon,-12,as_on_date) as prev_12_month,
        
        case 
            when view_day >= prev_3_month 
                and view_day < as_on_date
                then 1 
            else 0 
        end as short_term_flag,
        
        case 
            when view_day >= prev_12_month
                and view_day < prev_3_month
            then 1 
            else 0 
        end as mid_term_flag,
         
        case 
            when view_day < prev_12_month
                and view_day >= '2015-01-01'
            then 1 
            else 0 
        end as long_term_flag
    
    from cleaned
 
    where view_day < as_on_date

)

select * from with_flags




