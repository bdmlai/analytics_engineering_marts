{{
  config({
		"schema": 'fds_le',
		"materialized": 'incremental','tags': 'Phase 5A',"persist_docs": {'relation' : true, 'columns' : true}
  })
}}
select interval_start_date::varchar(10) as interval_start_date, state::varchar(5) as state, geography, 
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
from {{ref('intm_le_local_county_mapped')}} a
left join {{ref('intm_le_aggr_viewership')}} b 
on a.interval_start_date_id = b.interval_start_date_id and a.src_series_name = b.src_series_name)