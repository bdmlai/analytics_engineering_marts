{{
  config({
		"materialized": 'ephemeral'
  })
}}


SELECT DISTINCT
    A.time_key,
    A.Item_key,
    A.event_key,
    A.venue_key,
    SUM(A.quantity_shipped)    AS quantity_shipped,
    SUM(A.quantity_adjustment) AS quantity_adjustment ,
    SUM(A.quantity_returned)   AS quantity_returned,
    SUM(A.compelements)        AS compelements ,
    SUM(A.net_units_sold)      AS net_units_sold ,
    SUM(A.selling_price)       AS Selling_price,
    SUM(total_revenue)         AS total_revenue,
    SUM(A.complement_revenue)  AS complement_revenue,
    'V'                        AS Flag
FROM
    (
        SELECT
            CAST(TO_CHAR(event_dttm,'YYYYMMDD')AS bigint)    time_key,
            UPPER(ltrim(RTRIM(di.src_item_id)))           AS Item_key,
            de.dim_event_id                                  event_key,
            v.dim_venue_id                                AS venue_key,
            f552.wwe_quantity_shipped_qd_qshp             AS quantity_shipped,
            f552.wwe_quantity_adjusted_qd_qadj            AS quantity_adjustment,
            f552.wwe_quantity_returned_qd_qret            AS quantity_returned,
            f552.wwe_quantity_comp_qd_qcmp                AS compelements,
            (((f552.wwe_quantity_shipped_qd_qshp + f552.wwe_quantity_adjusted_qd_qadj) -
            f552.wwe_quantity_returned_qd_qret) - f552.wwe_quantity_comp_qd_qcmp) AS net_units_sold
            ,
            (((((((f552.wwe_quantity_shipped_qd_qshp + f552.wwe_quantity_adjusted_qd_qadj) -
            f552.wwe_quantity_returned_qd_qret) - f552.wwe_quantity_comp_qd_qcmp))::NUMERIC)::
            NUMERIC (9,2) / (
                CASE
                    WHEN ((f552.wwe_quantity_shipped_qd_qshp + f552.wwe_quantity_adjusted_qd_qadj)
                            = 0)
                    THEN NULL
                    ELSE (f552.wwe_quantity_shipped_qd_qshp + f552.wwe_quantity_adjusted_qd_qadj)
                END)::NUMERIC))::NUMERIC(9,2) AS selling_ratio,
            (((CAST(f552.amount_price_per_unit_qduprc AS NUMERIC(28,6))/ 10000))::NUMERIC)::NUMERIC
            (9,2) AS Selling_price,
            (((((f552.wwe_quantity_shipped_qd_qshp + f552.wwe_quantity_adjusted_qd_qadj) -
            f552.wwe_quantity_returned_qd_qret) - f552.wwe_quantity_comp_qd_qcmp))::NUMERIC * ((
            (CAST(f552.amount_price_per_unit_qduprc AS NUMERIC(28,6))/ 10000))::NUMERIC)::NUMERIC(9
            ,2)) AS total_revenue,
            ((f552.wwe_quantity_comp_qd_qcmp)::NUMERIC * (((CAST(f552.amount_price_per_unit_qduprc
            AS NUMERIC(28,6)) / 10000))::NUMERIC)::NUMERIC(9,2)) AS complement_revenue
        FROM
            ((((((({{source('hive_udl_cpg','jde_daily_merch_settlement_detail_f55m002')}} f552
        JOIN
            {{source('hive_udl_cpg','jde_daily_merch_settlement_header_f55m001')}} f551
        ON
            ((
                    UPPER(ltrim(RTRIM((f551.costcenter_qamcu)))) = UPPER(ltrim(RTRIM(
                    (f552.cost_center_qdmcu))))))
        AND f552.as_on_date = to_char(current_date,'YYYYMMDD')::BIGINT        AND f551.as_on_date = to_char(current_date,'YYYYMMDD')::BIGINT )
        JOIN
            {{source('hive_udl_cpg','jde_daily_business_unit_master_f0006')}} f6
        ON
            ((
                    UPPER(ltrim(RTRIM((f6.costcenter_mcmcu)))) = UPPER(ltrim(RTRIM(
                    (f552.cost_center_qdmcu))))))
        AND f552.as_on_date = to_char(current_date,'YYYYMMDD')::BIGINT         AND f6.as_on_date = to_char(current_date,'YYYYMMDD')::BIGINT )
        JOIN
            {{source('hive_udl_cpg','jde_daily_item_cost_f4105')}} f45
        ON
            ((((
                            UPPER(RTRIM(ltrim((f45.cost_center_comcu)))) = UPPER(ltrim(RTRIM(
                            (f552.cost_center_qdmcu)))))
                    AND ((
                                f45.identifier_2nd_item_colitm) = (f552.identifier_2nd_item_qdlitm)
                        ))
                AND ((
                            f45.cost_method_coledg) = '07')))
        AND f552.as_on_date = to_char(current_date,'YYYYMMDD')::BIGINT       AND f45.as_on_date = to_char(current_date,'YYYYMMDD')::BIGINT )
        JOIN
            {{source('hive_udl_cpg','jde_daily_item_master_f4101')}} f41
        ON
            (((
                        f41.identifier_2nd_item_imlitm) = (f552.identifier_2nd_item_qdlitm)))
        AND f552.as_on_date = to_char(current_date,'YYYYMMDD')::BIGINT       AND f41.as_on_date = to_char(current_date,'YYYYMMDD')::BIGINT )
        JOIN
            {{source('hive_udl_pii','restricted_jde_daily_address_by_date_f0116')}} f16
        ON
            ((
                    f16.address_number_alan8 = f6.addressnumber_mcan8))
        AND f16.as_on_date = to_char(current_date,'YYYYMMDD')::BIGINT       AND f6.as_on_date = to_char(current_date,'YYYYMMDD')::BIGINT )
        JOIN
            {{source('hive_udl_cpg','jde_daily_user_defined_codes_f0005')}} f5
        ON
            ((((
                            ltrim(RTRIM((f5.user_defined_code_drky))) = substring(
                            (f552.identifier_2nd_item_qdlitm), 1, 3))
                    AND ((
                                f5.product_code_drsy) = '41'))
                AND ((
                            f5.user_defined_codes_drrt) = 'S1')))
        AND f552.as_on_date = to_char(current_date,'YYYYMMDD')::BIGINT       AND f5.as_on_date = to_char(current_date,'YYYYMMDD')::BIGINT )
        JOIN
            {{source('hive_udl_cpg','jde_daily_user_defined_codes_f0005')}} f15
        ON
            ((((
                            ltrim(RTRIM((f15.user_defined_code_drky))) =
                            (f41.sales_category_code_2_imsrp2))
                    AND ((
                                f15.product_code_drsy) = '41'))
                AND ((
                            f15.user_defined_codes_drrt) = 'S2')))
        AND f15.as_on_date = to_char(current_date,'YYYYMMDD')::BIGINT       AND f41.as_on_date = to_char(current_date,'YYYYMMDD')::BIGINT )
        JOIN
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
            di.src_item_id = UPPER(LTRIM(RTRIM(f41.identifier_2nd_item_imlitm)))
        AND UPPER(di.active_flag) = 'Y'
        JOIN
            {{source('cdm','dim_event')}} de
        ON
            UPPER(LTRIM(RTRIM(de.event_src_sys_id))) = UPPER(LTRIM(RTRIM(f6.costcenter_mcmcu)))
        INNER JOIN
            {{source('fds_le','brdg_live_event_venue')}} v
        ON
            v.dim_event_id = de.dim_event_id ) A
GROUP BY
    A.time_key,
    A.Item_key,
    A.event_key,
    A.venue_key 