{{
  config({
		"materialized": 'ephemeral'
  })
}}

SELECT    a.show_date,
          a.show_name,
          storyline_1,
          Listagg(DISTINCT a.comment, ';') AS comments,
          -- Changed the format for the appeared column from a 24HR format to a 12HR format , Jira : PSTA-2949
          Listagg(DISTINCT Substring(Dateadd(hr,-12,a.inpoint_est),12,5)||' - '||Substring(Dateadd(hr,-12,a.outpoint_est),12,5),';')  AS appeared ,
          Listagg(DISTINCT a.talent,'|')    AS talent,
          (Avg(p18_49viewership)   / Avg(c.average_total_p18_49) :: decimal(10,2)) AS average_viewers_p18_49 ,
          (avg(a.p2plusviewership_000) / avg(c.avg_total_p2_plus) ::    decimal(10,2)) AS avg_p2_plus_viewership ,
          sum(nielsen_twitter_interactions) AS agg_nielsen_tw_interactions,
          row_number () OVER (partition BY a.show_date,a.show_name ORDER BY agg_nielsen_tw_interactions DESC) AS tw_interactions_ranking
FROM {{ref('intm_cp_nielsen_social_litelog')}} a 
LEFT JOIN          
(                    
SELECT    show_date,   
          show_name, 
		  AVG(p2plusviewership_000) AS avg_total_p2_plus , 
		  AVG(P18_49viewership) AS average_total_p18_49   
FROM     {{ref('intm_cp_nielsen_social_litelog')}}
GROUP BY 1,2
) c ON a.show_date=c.show_Date AND lower(a.show_name)=lower(c.show_name) 
GROUP BY  1,2,3