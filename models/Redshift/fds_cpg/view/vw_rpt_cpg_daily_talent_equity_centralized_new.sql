{{
  config({
		 'schema': 'fds_cpg',"materialized": 'view',"tags": 'monthly_talent_scorecard',
		 "persist_docs": {'relation' : true, 'columns' : true},
		 "post-hook": 'grant select on {{this}} to public'
        })
}}

SELECT  date 
       ,lineage_name 
       ,brand 
       ,SUM(tot_views)       AS tot_views 
       ,SUM(tot_yt_vids)     AS tot_yt_vids 
       ,SUM(tot_eng_fb_post) AS tot_eng_fb_post 
       ,SUM(tot_eng_fb_vids) AS tot_eng_fb_vids 
       ,SUM(tot_eng_tw_post) AS tot_eng_tw_post 
       ,SUM(tot_eng_tw_vids) AS tot_eng_tw_vids 
       ,SUM(mentions)        AS mentions
FROM 
(
	SELECT  date 
	       ,lineage_name 
	       ,brand 
	       ,tot_views 
	       ,tot_yt_vids 
	       ,tot_eng_fb_post 
	       ,tot_eng_fb_vids 
	       ,tot_eng_tw_post 
	       ,tot_eng_tw_vids 
	       ,mentions
	FROM {{source 
	('fds_cpg', 'rpt_daily_talent_equity_centralized' 
	)}}
	GROUP BY  1 
	         ,2 
	         ,3 
	         ,4 
	         ,5 
	         ,6 
	         ,7 
	         ,8 
	         ,9 
	         ,10 
)group by 1,2,3