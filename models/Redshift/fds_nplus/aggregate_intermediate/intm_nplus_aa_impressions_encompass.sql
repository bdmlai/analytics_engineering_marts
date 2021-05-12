{{ config({ "materialized": 'ephemeral' }) }}SELECT substring(CAST(strt_dt AS varchar) || ' ' || CAST(strt_tm AS varchar), 1, 19) AS start_time_join, CAST(strt_dt AS varchar) AS Start_Date,substring(strt_tm, 1, 8) AS Start_Time,upper(cntnd_id) Content_ID,
    CASE
    WHEN trim(cntnt_desc) LIKE '%wwe%' THEN
    replace(initcap(trim(cntnt_desc)),'Wwe','WWE')
    ELSE initcap(trim(cntnt_desc))
    END Content_Description, duratn Duration,substring(CAST(end_tm AS varchar),1,19) AS End_Time, upper(substring(cntnd_id, 1,4)) AS Ad_Abbreviation,
    CASE
    WHEN substring(cntnd_id, 1,4)= 'wwad'
        OR substring(cntnd_id, 1,4)='wwbb' THEN
    'Billboards'
    WHEN substring(cntnd_id, 1,4)='wwid' THEN
    'Brand IDs'
    WHEN substring(cntnd_id, 1,4)='wwxp' THEN
    'Cross Promotion'
    WHEN substring(cntnd_id, 1,4)='wwin' THEN
    'Interstitials'
    WHEN substring(cntnd_id, 1,4)='wwop' THEN
    'Miscellaneous'
    WHEN substring(cntnd_id, 1,4)='wwpr' THEN
    'Promos and Tune-Ins'
    ELSE 'Other'
    END AS ad_category,
    CASE
    WHEN cntnd_id LIKE '%wwb%'
        OR cntnd_id LIKE '%wwad%' THEN
    'Client Advertising'
    WHEN cntnt_desc LIKE '%komen%'
        OR cntnt_desc LIKE '%special olympics%'
        OR cntnt_desc LIKE '%ad council%'
        OR cntnt_desc LIKE '%reading challenge%'
        OR cntnt_desc like'%characters unite%'
        OR cntnt_desc LIKE 'make a wish%'
        OR cntnt_desc LIKE '%bully%'
        OR cntnt_desc LIKE '%connor%' THEN
    'Community Relations (CSR)'
    WHEN cntnd_id LIKE '%wwpr%' THEN
    'WWE Promos'
    WHEN (cntnd_id LIKE '%wwxp%'
        OR cntnt_desc LIKE '%wom%s history%') THEN
    'Cross Promotion'
    WHEN (cntnd_id LIKE '%wwid%'
        OR cntnd_id LIKE '%wwin%'
        OR cntnd_id LIKE '%wwop%') THEN
    'WWE Promos'
    ELSE 'Other'
    END AS Ad_Type
FROM {{source('udl_nplus','encompass_as_run')}}
WHERE strt_dt
    BETWEEN '2017-01-01'
        AND current_date-1
        AND (cntnd_id NOT LIKE '%live%'
        AND cntnd_id NOT LIKE '%wwe%'
        AND cntnd_id NOT LIKE '%rem1%'
        AND cntnd_id NOT LIKE '%swit%'
        AND cntnd_id NOT LIKE '%wwra%') 