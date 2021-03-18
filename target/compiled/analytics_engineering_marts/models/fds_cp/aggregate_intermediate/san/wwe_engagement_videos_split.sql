
SELECT video_id,SPLIT_PART(talent,',',N) as name
FROM (select video_id,talent from __dbt__CTE__wwe_engagement_videos group by 1,2)
CROSS JOIN 
      (     
            SELECT N::INT 
            FROM 
            (
  SELECT ROW_NUMBER() OVER (ORDER BY TRUE) AS N FROM (select video_id,talent from __dbt__CTE__wwe_engagement_videos group by 1,2)
            )
      ) 
WHERE SPLIT_PART(talent,',',N) IS NOT NULL AND SPLIT_PART(talent,',',N) != '' 
group by 1,2