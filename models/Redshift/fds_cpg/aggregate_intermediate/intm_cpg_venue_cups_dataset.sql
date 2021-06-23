
{{
  config({
		"materialized": 'ephemeral'
  })
}}


SELECT DISTINCT
    CAST(TO_CHAR(de.event_dttm,'YYYYMMDD')AS bigint)    time_key,
    UPPER(ltrim(RTRIM(di.src_item_id)))              AS Item_key,
    de.dim_event_id                                     event_key,
    v.dim_venue_id                                   AS venue_key,
    0                                                AS quantity_shipped ,
    0                                                AS quantity_adjustment,
    0                                                AS quantity_returned,
    0                                                AS Compelements ,
    SUM(tab1.Qty_Sold)                               AS net_units_sold,
    SUM(Tab1.Selling_price)                          AS selling_price,
    SUM(tab1.amount)                                 AS total_revenue ,
    0                                                AS complement_revenue ,
    'C'                                              AS flag
FROM
    (
        SELECT
            cost_center_header_sdemcu Event,
            CAST(CAST(price_per_unit_amount_sduprc AS NUMERIC(28,6))/10000 AS DEC(13,4))
                                                        Selling_price,
            shipped_units_quantity_sdsoqs                                              Qty_Sold,
            item_number_2nd_sdlitm                                                     Item,
            description_line_1_sddsc1                                              Item_Description,
            CAST(CAST(extended_price_amount_sdaexp AS NUMERIC(28,6))/100 AS DEC(13,2)) Amount
        FROM
            {{source('hive_udl_cpg','jde_daily_sales_order_history_f42119')}}
        WHERE
            status_code_last_sdlttr < '980'
        AND trim(cost_center_header_sdemcu) IN
            (
                SELECT
                    trim(costcenter_mcmcu)
                FROM
                    {{source('hive_udl_cpg','jde_daily_business_unit_master_f0006')}}
                WHERE
                    costcentertype_mcstyl = 'EV'
                AND as_on_date = to_char(current_date,'YYYYMMDD')::BIGINT)
        AND item_number_2nd_sdlitm IN
            (
                SELECT
                    identifier_2nd_item_imlitm
                FROM
                    {{source('hive_udl_cpg','jde_daily_item_master_f4101')}}
                WHERE
                    description_line_1_imdsc1 LIKE '%Cup%'
                OR  description_line_1_imdsc1 LIKE '%CUP%'
                AND as_on_date = to_char(current_date,'YYYYMMDD')::BIGINT)
        AND as_on_date = to_char(current_date,'YYYYMMDD')::BIGINT)Tab1
INNER JOIN
    (
        SELECT
            *
        FROM
            {{source('fds_cpg','dim_cpg_item')}}
        WHERE
            dim_business_unit_id IN
            (
                SELECT
                    dim_business_unit_id
                FROM
                    {{source('fds_cpg','dim_cpg_business_unit')}}
                WHERE
                    src_business_unit_id='W03')) di
ON
    UPPER(ltrim(RTRIM(di.src_item_id))) = UPPER(ltrim(RTRIM(tab1.item)))
AND UPPER(di.active_flag) = 'Y'
JOIN
    {{source('cdm','dim_event')}} de
ON
    UPPER(ltrim(RTRIM(tab1.event))) = UPPER(ltrim(RTRIM(de.event_src_sys_id)))
INNER JOIN
    {{source('fds_le','brdg_live_event_venue')}} v
ON
    v.dim_event_id=de.dim_event_id
GROUP BY
    de.event_dttm,
    di.src_item_id,
    de.dim_event_id,
    v.dim_venue_id,
    quantity_shipped,
    quantity_adjustment,
    quantity_returned,
    Compelements,
    complement_revenue 