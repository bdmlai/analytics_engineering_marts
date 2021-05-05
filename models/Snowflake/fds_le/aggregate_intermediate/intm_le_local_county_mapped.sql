{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    a.*,
    (substring(interval_start_date_id, 1, 4) || '-' || substring(interval_start_date_id, 5, 2) ||
    '-' || substring(interval_start_date_id, 7, 2)) AS interval_start_date,
    CASE
        WHEN src_series_name ='WWE SMACKDOWN'
        THEN 'SMACKDOWN'
        ELSE 'RAW'
    END AS brand_name,
    b.*
FROM
    (
        SELECT
            *,
            AVG(ue) over(partition BY geography, src_series_name ORDER BY interval_start_date_id
            ASC rows 2 preceding) AS ue_3m_avg,
            AVG(imp) over(partition BY geography, src_series_name ORDER BY interval_start_date_id
            ASC rows 2 preceding) AS imp_3m_avg
        FROM
            {{ref('intm_le_local_viewership')}}) a
LEFT JOIN
    (
        SELECT
            *
        FROM
            (
                SELECT
                    *,
                    rank() over (partition BY state, county_name ORDER BY zip_code_count DESC) AS
                    rank
                FROM
                    (
                        SELECT
                            state,
                            dma_name,
                            country_name             AS county_name,
                            COUNT(DISTINCT zip_code) AS zip_code_count
                        FROM
                            {{source('prod_entdwdb.udl_nl','nielsen_mapping_ziptodma')}}
                        GROUP BY
                            state,
                            country_name,
                            dma_name))
        WHERE
            rank = 1) b
ON
    trim(LOWER(a.geography)) = trim(LOWER(b.dma_name))