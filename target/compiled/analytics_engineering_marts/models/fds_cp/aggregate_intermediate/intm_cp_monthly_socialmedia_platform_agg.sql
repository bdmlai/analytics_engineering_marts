

select date as month,country,
 'facebook_followers' as metric,'NA' as page ,
   fb_followers as values,'Social Media' as platform
   from __dbt__CTE__intm_cp_monthly_socialmedia_platform
   
   union all
   
   select date as month,country,
 'instagram_followers' as metric,'NA' as page ,
   igm_followers as values,'Social Media' as platform
   from __dbt__CTE__intm_cp_monthly_socialmedia_platform
   
   union all
   select date as month,country,
 'youtube_followers' as metric,'NA' as page ,
   yt_followers as values,'Social Media' as platform
   from __dbt__CTE__intm_cp_monthly_socialmedia_platform

   union all
   
   select month,country,metric,page,values,platform from __dbt__CTE__intm_cp_monthly_socialmedia_platform_twitter