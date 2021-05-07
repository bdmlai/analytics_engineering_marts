{{
  config({
		 'schema': 'fds_cp',	
	     "materialized": 'view',"tags": 'Content',"persist_docs": {'relation' : true, 'columns' : true},
		 "post-hook" : 'grant select on {{this}} to public'
        })
}}

select 
c.country,
a.date,
avg(c.population) as population,
sum(a.fb_gain) as fb_gain,
sum(a.fb_followers) as fb_followers,
sum(b.igm_gain) as igm_gain,
sum(b.igm_followers) as igm_followers,
sum(d.yt_gain) as yt_gain,
sum(d.yt_followers) as yt_followers
from
(select date,dim_country_id,fb_gain,fb_followers from {{ref('intm_cp_fb_followers_gain')}} )a
 left join 
(select date,dim_country_id,igm_followers,igm_gain from {{ref('intm_cp_ig_followers_gain')}} )b 
 on  a.date=b.date and a.dim_country_id=b.dim_country_id
 left join 
(select dim_country_id ,Country,Population from {{ref('intm_cp_country_population')}} )c
 on  a.dim_country_id = c.dim_country_id
 left join
(select date,country,region,yt_gain,yt_followers from  {{ref('intm_cp_yt_followers_gain')}} )d
 on  a.date = d.date and upper(c.country)=upper(d.country)
group by 1,2
order by 1,2
