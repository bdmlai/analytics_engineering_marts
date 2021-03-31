{{
  config({
		"materialized": 'ephemeral'
  })
}}

     
   
            SELECT substring(CAST(strt_dt as varchar) || ' ' || CAST(strt_tm as varchar), 1, 19) as start_time_join,
            CAST(strt_dt as varchar) as Start_Date,substring(strt_tm, 1, 8) as Start_Time,upper(cntnd_id) Content_ID,
            CASE when trim(cntnt_desc) like '%wwe%' then replace(initcap(trim(cntnt_desc)),'Wwe','WWE')
            else initcap(trim(cntnt_desc)) end Content_Description,
            duratn Duration,substring(CAST(end_tm as varchar),1,19) as End_Time,
            upper(substring(cntnd_id, 1,4)) as Ad_Abbreviation,
            CASE WHEN substring(cntnd_id, 1,4)= 'wwad' or substring(cntnd_id, 1,4)='wwbb' THEN 'Billboards'
            WHEN substring(cntnd_id, 1,4)='wwid' THEN 'Brand IDs'
            WHEN substring(cntnd_id, 1,4)='wwxp' THEN 'Cross Promotion'
            WHEN substring(cntnd_id, 1,4)='wwin' THEN 'Interstitials'
            WHEN substring(cntnd_id, 1,4)='wwop' THEN 'Miscellaneous'
            WHEN substring(cntnd_id, 1,4)='wwpr' THEN 'Promos and Tune-Ins'
            ELSE 'Other' END as ad_category,
            CASE WHEN cntnd_id like '%wwb%' or cntnd_id like '%wwad%' THEN 'Client Advertising'
            WHEN cntnt_desc like '%komen%' or cntnt_desc like '%special olympics%' or cntnt_desc like '%ad council%' or cntnt_desc like '%reading challenge%'
            or cntnt_desc like'%characters unite%' or cntnt_desc like 'make a wish%' or cntnt_desc like '%bully%' or cntnt_desc like '%connor%' THEN 'Community Relations (CSR)'
            WHEN cntnd_id like '%wwpr%' THEN 'WWE Promos'
            WHEN (cntnd_id like '%wwxp%' or cntnt_desc like '%wom%s history%') THEN 'Cross Promotion'
            WHEN (cntnd_id like '%wwid%' or cntnd_id like '%wwin%' or cntnd_id like '%wwop%') THEN 'WWE Promos'
            ELSE 'Other' END AS Ad_Type
            FROM {{source('udl_nplus','encompass_as_run')}}
            WHERE strt_dt between '2017-01-01' and current_date-1
            and (cntnd_id not like '%live%' and cntnd_id not like '%wwe%' and cntnd_id not like '%rem1%' and cntnd_id not like '%swit%' and cntnd_id not like '%wwra%')
