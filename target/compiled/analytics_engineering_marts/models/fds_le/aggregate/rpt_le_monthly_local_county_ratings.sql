
with __dbt__CTE__intm_le_local_viewership as (

select interval_start_date_id, src_series_name, geography, 
avg(rtg_percent) as rtg_percent, sum(ue::decimal(20,4)) as ue, sum(imp::decimal(20,4)) as imp 
from "prod_entdwdb"."fds_nl"."fact_nl_monthly_local_market"
where src_playback_period_desc = 'Live+Same Day' and src_series_name in ('WWE SMACKDOWN','WWE ENTERTAINMENT') 
and interval_start_date_id >= 20190101 and src_demographic_group='TV Households P2+'
group by 1,2,3
),  __dbt__CTE__intm_le_local_county_mapped as (

select a.*, (substring(interval_start_date_id, 1, 4) || '-' || substring(interval_start_date_id, 5, 2) 
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
),  __dbt__CTE__intm_le_aggr_viewership as (

select *, avg(ue) over(partition by src_series_name order by interval_start_date_id asc rows 2 preceding) as ue_3m_avg_nat,
avg(imp) over(partition by src_series_name order by interval_start_date_id asc rows 2 preceding) as imp_3m_avg_nat 
from
(select interval_start_date_id, src_series_name, (100.0000*sum(imp)/nullif(sum(ue), 0)) as avg_audience_pct, 
sum(ue) as ue, sum(imp) as imp 
from __dbt__CTE__intm_le_local_viewership
group by 1,2)
)select interval_start_date::varchar(10) as interval_start_date, state::varchar(5) as state, geography, 
dma_name::varchar(255) as dma_name, county_name::varchar(255) as county_name, 
src_series_name, brand_name, zip_code_count::int as zip_code_count, rtg_percent::decimal(32,4) as rtg_percent, 
ue::int as ue, imp::int as imp, avg_audience_pct::decimal(32,4) as avg_audience_pct, 
(rtg_percent/nullif(avg_audience_pct, 0))::decimal(32,4) as index, imp_3m_avg::decimal(32,4) as viewers_3m_avg,
case 
when src_series_name = 'WWE ENTERTAINMENT' then ((imp_3m_avg/nullif(ue_3m_avg, 0))/(imp_3m_avg_nat/nullif(ue_3m_avg_nat, 0)))::decimal(32,4)
end as raw_index_3day_moving_avg,
case 
when src_series_name = 'WWE SMACKDOWN' then (rtg_percent/nullif(avg_audience_pct, 0))::decimal(32,4)
end as SD_index,
case 
when src_series_name = 'WWE ENTERTAINMENT' then (rtg_percent/nullif(avg_audience_pct, 0))::decimal(32,4)
end as RAW_index,
case 
when interval_start_date <= '2020-02-01' then 'Wave B' 
else 'Wave A' 
end as wave_flag,
('DBT_' || TO_CHAR(((convert_timezone ('UTC', getdate()))::timestamp),'YYYY_MM_DD_HH24_MI_SS') || '_5A')::varchar(255) as etl_batch_id, 
'SVC_PROD_BI_DBT_USER' as etl_insert_user_id, ((convert_timezone ('UTC', getdate()))::timestamp) as etl_insert_rec_dttm, 
null::varchar(255) as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from
(select a.*, b.avg_audience_pct, b.ue_3m_avg_nat, b.imp_3m_avg_nat 
from __dbt__CTE__intm_le_local_county_mapped a
left join __dbt__CTE__intm_le_aggr_viewership b 
on a.interval_start_date_id = b.interval_start_date_id and a.src_series_name = b.src_series_name)