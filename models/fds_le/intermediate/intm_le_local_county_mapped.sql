{{
  config({
		"materialized": 'ephemeral'
  })
}}
select a.*, (substring(interval_start_date_id, 1, 4) || '-' || substring(interval_start_date_id, 5, 2) 
|| '-' || substring(interval_start_date_id, 7, 2)) as interval_start_date,
case 
when src_series_name ='WWE SMACKDOWN' then 'SMACKDOWN'
else 'RAW'
end as brand_name, b.*
from
(select *, avg(ue) over(partition by geography, src_series_name order by interval_start_date_id asc rows 2 preceding) as ue_3m_avg,
avg(imp) over(partition by geography, src_series_name order by interval_start_date_id asc rows 2 preceding) as imp_3m_avg 
from {{ref('intm_le_local_viewership')}}) a 
left join 
(select * 
from 
(select *, rank() over (partition by state, county_name order by zip_code_count desc) as rank 
from
(select state, dma_name, country_name as county_name, count(distinct zip_code) as zip_code_count 
from {{source('sf_udl_nl','nielsen_mapping_ziptodma')}}
group by state, country_name, dma_name))
where rank = 1) b on trim(lower(a.geography)) = trim(lower(b.dma_name))