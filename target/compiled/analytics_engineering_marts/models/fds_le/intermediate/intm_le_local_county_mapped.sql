
with __dbt__CTE__intm_le_local_viewership as (

select interval_start_date_id, src_series_name, geography, 
avg(rtg_percent) as rtg_percent, sum(ue::decimal(20,4)) as ue, sum(imp::decimal(20,4)) as imp 
from "prod_entdwdb"."fds_nl"."fact_nl_monthly_local_market"
where src_playback_period_desc = 'Live+Same Day' and src_series_name in ('WWE SMACKDOWN','WWE ENTERTAINMENT') 
and interval_start_date_id >= 20190101 and src_demographic_group='TV Households P2+'
group by 1,2,3
)select a.*, (substring(interval_start_date_id, 1, 4) || '-' || substring(interval_start_date_id, 5, 2) 
|| '-' || substring(interval_start_date_id, 7, 2)) as interval_start_date,
case 
when src_series_name ='WWE SMACKDOWN' then 'SMACKDOWN'
else 'RAW'
end as brand_name, b.*
from
(select *, avg(ue) over(partition by geography, src_series_name order by interval_start_date_id asc rows 2 preceding) as ue_3m_avg,
avg(imp) over(partition by geography, src_series_name order by interval_start_date_id asc rows 2 preceding) as imp_3m_avg 
from __dbt__CTE__intm_le_local_viewership) a 
left join 
(select * 
from 
(select *, rank() over (partition by state, county_name order by zip_code_count desc) as rank 
from
(select state, dma_name, country_name as county_name, count(distinct zip_code) as zip_code_count 
from "prod_entdwdb"."udl_nl"."nielsen_mapping_ziptodma"
group by state, country_name, dma_name))
where rank = 1) b on trim(lower(a.geography)) = trim(lower(b.dma_name))