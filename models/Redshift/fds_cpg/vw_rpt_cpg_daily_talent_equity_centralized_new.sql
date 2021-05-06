{{
  config({
		 'schema': 'fds_cpg',"materialized": 'view',"tags": 'monthly_talent_scorecard',
		 "persist_docs": {'relation' : true, 'columns' : true},
		 "post-hook": 'grant select on {{this}} to public'
        })
}}

select date,lineage_name,brand,
sum(tot_views) as tot_views,
sum(tot_yt_vids) as tot_yt_vids,
sum(tot_eng_fb_post) as tot_eng_fb_post,
sum(tot_eng_fb_vids) as tot_eng_fb_vids,
sum(tot_eng_tw_post) as tot_eng_tw_post,
sum(tot_eng_tw_vids) as tot_eng_tw_vids,
sum(mentions) as mentions
 from(
select date, lineage_name,brand, tot_views, tot_yt_vids, tot_eng_fb_post, tot_eng_fb_vids, tot_eng_tw_post, tot_eng_tw_vids,mentions
from {{source('fds_cpg', 'rpt_daily_talent_equity_centralized')}}
group by 1,2,3,4,5,6,7,8,9,10)group by 1,2,3

