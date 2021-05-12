{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    *
FROM
    (
        SELECT DISTINCT
            src_item_id                                                         AS item_id,
            src_item_description                                                AS item_description,
            src_royalty_name                                                    AS royalty_code,
            row_number() over (partition BY src_item_id ORDER BY effective_start_datetime DESC) AS
            row_num
        FROM
            {{source('fds_cpg','dim_cpg_item')}}
        WHERE
            active_flag='Y')
WHERE
    row_num=1