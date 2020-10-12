/***********************************************************************************/
/*                     tier 3 model - content cluster base                         */
/*                    db:analytics_workspace schema:content                        */
/* input tables: cdm.dim_content_classification_title                              */
/*               fds_nplus.aggr_daily_postshow_rankings                            */
/*               fds_nplus.fact_daily_content_viewership                           */
/* output table: al_t3_vc_content_cluster                                          */
/***********************************************************************************/
/*-----------------------------------------------------------*/
/*                preparing video meta deta                  */
/*-----------------------------------------------------------*/
-- meta data for all video content
drop table if exists meta;
create temporary table meta as
select production_id,
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
       case when premiere_date < '1964-01-01' then 'capitol era'
            when premiere_date between '1964-01-01' and '1977-04-29' then 'msg era'
            when premiere_date between '1977-04-30' and '1984-01-22' then 'titan era'
            when premiere_date between '1984-01-23' and '1992-10-11' then 'golden era'
            when premiere_date between '1992-10-12' and '1997-12-14' then 'the new generation era'
            when premiere_date between '1997-12-15' and '2002-01-23' then 'attitude era'
            when premiere_date between '2002-01-24' and '2008-07-19' then 'ruthless aggression'
            when premiere_date between '2008-07-20' and '2011-06-26' then 'attitude adjustment era'
            when premiere_date between '2011-06-27' and '2016-04-03' then 'reality era'
            when premiere_date >= '2016-04-04' then 'new era'
          else 'unknown' end as wwe_era
  from prod_entdwdb.cdm.dim_content_classification_title;
  
-- append post show info (flagging debuting post shows)
  drop table if exists meta_2;
create temporary table meta_2 as
select t1.*,
       t2.post_rank_flag,
       t2.start_time
  from meta as t1
  left join (select distinct production_id, 
                             post_rank_flag,
                             encore,
                             start_est_dttm as start_time
               from prod_entdwdb.fds_nplus.aggr_daily_postshow_rankings 
              where encore <> 'x') as t2
    on t1.production_id = t2.production_id;

-- dedupe at production id level
  drop table if exists meta_3;
create temporary table meta_3 as
select * from 
        (select *,
                row_number() over (partition by production_id order by production_id desc) as duplicate_flag   --changed to production_id
           from meta_2
         )
  where duplicate_flag = 1;     
  
  
  
-- reclassifying all video content
  drop table if exists meta_final;
create temporary table meta_final as
select *,
       case when franchise_name is null then 'wwe' else franchise_name end as clean_franchise,
       case
        /* originals */
             -- toss out hall of fames, shoulder, specials
        when class_name = 'original' and lower(content_title) like '%hall of fame%' then 'toss out'
        when lower(series_group) like '%hall of fame%' then 'toss out'
        when class_name = 'original' and lower(category_name) in ('shoulder (original)','special') then 'toss out'
        when class_name = 'original' and lower(category_name) not in ('shoulder (original)','special') then category_name
        
        /* other toss outs (test streams, etc) */
        when content_title in('test','this is nxt 2017','wwe draft center live') then 'toss out'
        when class_name is null then 'toss out'
        when class_name = 'in-ring' and category_name = 'shoulder' and (event_style_name = 'episodic' or event_style_name is null) then 'toss out'
        when class_name = 'in-ring' and category_name = 'highlights' then 'toss out'
        when class_name = 'in-ring' and category_name in ('collections','promos') then 'toss out'
        
        /*  ppvs and kickoff shows */
        when class_name = 'in-ring' and category_name = 'shoulder' and event_style_name = 'ppv'
                                    and lower(series_group) in ('wwe ppv kickoff show') and lower(content_title) not like '%fallout%' 
                                    and (lower(content_title) like '%wrestlemania%'
                                         or lower(content_title) like '%royal rumble%'
                                         or lower(content_title) like '%summerslam%'
                                         or lower(content_title) like '%survivor series%')
                                    then 'tier 1 ppv'
        when class_name = 'in-ring' and category_name = 'shoulder' and event_style_name = 'ppv' 
                                    and lower(series_group) in ('wwe ppv kickoff show') and lower(content_title) not like '%fallout%'
                                    and (lower(content_title) not like '%wrestlemania%'
                                          and lower(content_title) not like '%royal rumble%'
                                          and lower(content_title) not like '%summerslam%'
                                          and lower(content_title) not like '%survivor series%')
                                     then 'tier 2 ppv'
        when class_name = 'in-ring' and category_name = 'event'  and event_style_name = 'ppv' 
                                    and lower(series_group) in ('wwe ppv') and lower(content_title) not like '%fallout%' 
                                    and (lower(content_title) like '%wrestlemania%'
                                         or lower(content_title) like '%royal rumble%'
                                         or lower(content_title) like '%summerslam%'
                                         or lower(content_title) like '%survivor series%')
                                    then 'tier 1 ppv'
        when class_name = 'in-ring' and category_name = 'event'    and event_style_name = 'ppv' 
                                    and lower(series_group) in ('wwe ppv') 
                                    and lower(content_title) not like '%fallout%'
                                    and (lower(content_title) not like '%wrestlemania%'
                                         and lower(content_title) not like '%royal rumble%'
                                         and lower(content_title) not like '%summerslam%'
                                          and lower(content_title) not like '%survivor series%')
                                     then 'tier 2 ppv'
      
        /* nxt takeover */
        when class_name = 'in-ring' and category_name = 'shoulder' and event_style_name = 'special' and lower(series_group)='nxt takeover - pre-show' then 'nxt takeover'
        when class_name = 'in-ring' and category_name = 'shoulder' and event_style_name = 'special' and lower(series_group)='nxt uk takeover - pre-show' then 'nxt takeover'
        when class_name = 'in-ring' and category_name = 'event' and event_style_name = 'special' and lower(series_group)= 'nxt takeover' then 'nxt takeover'
        when class_name = 'in-ring' and category_name = 'event' and event_style_name = 'special' and lower(series_group)= 'nxt uk takeover' then 'nxt takeover'
      
        /* in-rings */
        when class_name = 'in-ring' and category_name = 'event' and event_style_name = 'episodic' and lower(series_group) = '205 live' then '205 live'
        when class_name = 'in-ring' and category_name = 'event' and event_style_name = 'episodic' and lower(series_group) = 'nxt' then 'nxt'
        when class_name = 'in-ring' and category_name = 'event' and event_style_name = 'episodic' and lower(series_group) = 'nxt uk' then 'nxt uk'
        when class_name = 'in-ring' and category_name = 'event' and event_style_name = 'tournament' then 'in-ring tournament'
        
        /* special in-ring events */ 
        when (class_name = 'in-ring' and category_name = 'event' and event_style_name = 'special' and series_name = 'special event'
               and any_network_first_stream = first_stream)
             or production_id in ('star2018_ntwk','evolve10th_ntwk') then 'in-ring special'

        /* others */  
        when lower(series_group) in('raw','smackdown','wwe main event') then 'raw sd me tv replays'
        when lower(series_group) in('theme weeks','raw shoulder old') then 'toss out'
        when lower(series_group) in ('wwe raw talk','wwe talking smack', 'raw talk','talking smack') then 'toss out'
        
       else 'unclassified' end as first_class
  from meta_3;
  
  

/*-----------------------------------------------------------*/
/*                preparing stream base table                 */
/*-----------------------------------------------------------*/
-- add video meta data to network viewership data at stream level
  drop table if exists content_cluster;
create temporary table content_cluster as
select a.subs_tier,
       a.content_tier,
       a.stream_tier,
       a.stream_id,
       a.src_fan_id,
       a.stream_start_dttm as min_time,
       a.play_time,
       a.time_spent,
       to_date(a.stream_start_dttm) as view_day,
       a.stream_type_cd as stream_type,
       a.production_id as production_id_vt,
       b.*
  from (select * from prod_entdwdb.fds_nplus.fact_daily_content_viewership 
         where to_date(stream_start_dttm) >= '2019-01-01' and subs_tier = '3') as a  -- change the date range here
  left join meta_final as b
    on a.production_id = b.production_id;
  
-- further classifications  
  drop table if exists content_cluster_2;
create temporary table content_cluster_2 as
select *,
       (case  when first_stream is null then 'toss out'
              when first_class = 'tier 1 ppv' then 'tier 1 ppv'
              when first_class = 'tier 2 ppv' then 'tier 2 ppv'
              when first_class = 'nxt takeover' then 'nxt takeover'
              when first_class in ('205 live','nxt','in-ring tournament','nxt uk','in-ring special') then 'network in-ring'
              when first_class = 'toss out' then  'toss out'
              when first_class in ('animation','best of','documentary','event','lifestyle','news','reality','scripted series','talk show') then 'original'
              when first_class = 'unclassified' then 'historic in-ring'
          else 'unclassified' end) as clustering_1,
        (case when first_class = 'unclassified' and clean_franchise = 'wwe' then wwe_era
              when first_class = 'unclassified' and clean_franchise <> 'wwe' then clean_franchise
              when first_class <> 'unclassified' then ''
          else '' end) as historic_in_ring_class 
from content_cluster;


-- clean up historic in-ring and network originals classification
drop table if exists content_cluster_3;
create temporary table content_cluster_3 as
select *,
  -- for in-ring and big events, if the show was viewed after 90 days from debut, then reclassify them into historic in-ring
  (case when first_class in ('tier 1 ppv','tier 2 ppv') and datediff(d,any_network_first_stream,min_time) > 90 then wwe_era 
        when first_class = 'nxt takeover' and datediff(d,any_network_first_stream,min_time) > 90 then 'historic nxt takeover' 
        when first_class = 'nxt' and datediff(d,any_network_first_stream,min_time) > 90 then 'historic nxt' 
        when first_class = 'nxt uk' and datediff(d,any_network_first_stream,min_time) > 90 then 'historic nxt uk' 
        when first_class = '205 live' and datediff(d,any_network_first_stream,min_time) > 90 then 'historic 205 live' 
        when first_class = 'in-ring tournament' and datediff(d,any_network_first_stream,min_time) > 90 then 'historic in-ring tournament' 
        when first_class = 'in-ring special' and datediff(d,any_network_first_stream,min_time) > 90 then 'historic in-ring special'  
        when first_class = 'raw sd me tv replays' and datediff(d,any_network_first_stream,min_time) > 90 then wwe_era 
        
        -- sept-2019 reclassify originals based on premiere date and view day
          -- premiering on a platform other than network first
        when clustering_1 = 'original' and any_network_first_stream < to_date(first_stream) - 7 then 'historic original'
          -- viewed after 90 days of its debut date
        when clustering_1 = 'original' and view_day > to_date(first_stream) + 90 then 'network historic original'
          -- viewed within 90 days of its debut date
        when clustering_1 = 'original' and view_day <= to_date(first_stream) + 90 then 'network new original'

        when clustering_1 = 'historic in-ring' then historic_in_ring_class
      else first_class end) as content_class_0,

  -- toss out some linear consumption, keeps big events and in-rings
  (case when first_class in ('tier 1 ppv','tier 2 ppv','nxt takeover','205 live','nxt','nxt uk','in-ring tournament','in-ring special') 
                 and stream_type = 'linear' then play_time
        when post_rank_flag in ('post-raw','post-smackdown','post-nxt') and stream_type = 'linear' and to_date(min_time) = to_date(start_time) then play_time
        when stream_type = 'vod' then play_time
      else 0 end) as cluster_time
from content_cluster_2;


-- add flags to indicate the which stream level would be included into models with different timeframes
drop table if exists al_t3_vc_content_cluster_temp;
create temporary table al_t3_vc_content_cluster_temp as
select *,
  -- regroup historic in-ring
       case when content_class_0 in ('historic 205 live', 'historic nxt','historic nxt takeover',
                                     'historic in-ring tournament','historic nxt uk','historic in-ring special') then 'historic network in-ring'
            when content_class_0 in ('awa','gwf','jcp', 'msw','smw','wwwf','wccw','stampede') then 'indie wrestling'
            when content_class_0 in ('golden era','titan era','capitol era','msg era') then 'golden era'
            when content_class_0 in ('attitude adjustment era','reality era') then 'reality era'
          else content_class_0 end as content_class,
   -- regroup content_class into high level categories
       case when content_class in ('tier 1 ppv','tier 2 ppv','nxt takeover') then 'big event'
            when clustering_1 = 'original' then 'original'
            when content_class in ('205 live','nxt','nxt uk','in-ring special','in-ring tournament') then 'network in-ring'
            when content_class in ('attitude era','ecw','golden era','historic network in-ring','indie wrestling',
                                   'new era','reality era','ruthless aggression',
                                   'the new generation era','wcw') then 'historic in-ring'
            else 'toss out' end as content_category,
         to_date(to_date(to_char(current_date),'yyyy-mm-dd')) as today_date,
         -- add flags to indiate the timeframes
         dateadd(mon,-3,today_date) as prev_3_month,
         dateadd(mon,-12,today_date) as prev_12_month,
         case when view_day >= prev_3_month 
            and view_day < today_date
            then 1 else 0 end as short_term_flag,
         case when view_day >= prev_12_month
             and view_day < prev_3_month
            then 1 else 0 end as mid_term_flag,
         case when view_day < prev_12_month
             and view_day >= '2015-01-01'
            then 1 else 0 end as long_term_flag
  from content_cluster_3
 where view_day < today_date;

drop table if exists al_t3_vc_content_cluster;
create table al_t3_vc_content_cluster as select * from al_t3_vc_content_cluster_temp;
grant select on al_t3_vc_content_cluster to public;
select top 1
trunc(dateadd(mon,-3,today_date)) as prev_3_month,

select top 100* from content_cluster_3;

