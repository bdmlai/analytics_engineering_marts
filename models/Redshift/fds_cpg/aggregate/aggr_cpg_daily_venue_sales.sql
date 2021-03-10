{{
  config({
		'schema': 'fds_cpg',
		"pre-hook": ["
		DROP TABLE IF EXISTS #TEMP_V;
		DROP TABLE IF EXISTS #TEMP_C;
		DROP TABLE IF EXISTS #TEMP_R;
		SELECT DISTINCT 
		A.time_key,
		A.Item_key,
		A.event_key,
		A.venue_key,
		SUM(A.quantity_shipped) AS quantity_shipped,
		SUM(A.quantity_adjustment) AS quantity_adjustment  ,
		SUM(A.quantity_returned) AS quantity_returned,
		SUM(A.compelements) AS compelements ,
	    SUM(A.net_units_sold) AS net_units_sold ,
	    SUM(A.selling_price) AS Selling_price,
	    SUM(total_revenue) AS total_revenue,
        SUM(A.complement_revenue) AS complement_revenue,
		'V' as Flag
		INTO #TEMP_V
		FROM (
		SELECT
        CAST(to_char(event_dttm,'YYYYMMDD')as bigint) time_key,
        upper(ltrim(rtrim(di.src_item_id))) as Item_key,
        de.dim_event_id event_key,
        v.dim_venue_id as venue_key,  
        f552.wwe_quantity_shipped_qd_qshp     AS quantity_shipped,
		f552.wwe_quantity_adjusted_qd_qadj    AS quantity_adjustment,
		f552.wwe_quantity_returned_qd_qret    AS quantity_returned,
		f552.wwe_quantity_comp_qd_qcmp        AS compelements,
		(((f552.wwe_quantity_shipped_qd_qshp + f552.wwe_quantity_adjusted_qd_qadj) - f552.wwe_quantity_returned_qd_qret) - f552.wwe_quantity_comp_qd_qcmp)       AS net_units_sold,
		(((((((f552.wwe_quantity_shipped_qd_qshp + f552.wwe_quantity_adjusted_qd_qadj) - f552.wwe_quantity_returned_qd_qret) - f552.wwe_quantity_comp_qd_qcmp))::NUMERIC)::NUMERIC
		(9,2) / (
        CASE
            WHEN ((f552.wwe_quantity_shipped_qd_qshp + f552.wwe_quantity_adjusted_qd_qadj) = 0)
            THEN NULL
            ELSE (f552.wwe_quantity_shipped_qd_qshp + f552.wwe_quantity_adjusted_qd_qadj)
            END)::NUMERIC))::NUMERIC(9,2)      AS selling_ratio,
            (((cast(f552.amount_price_per_unit_qduprc as  NUMERIC(28,6))/ 10000))::NUMERIC)::NUMERIC(9,2) AS Selling_price,
            (((((f552.wwe_quantity_shipped_qd_qshp + f552.wwe_quantity_adjusted_qd_qadj) - f552.wwe_quantity_returned_qd_qret) - f552.wwe_quantity_comp_qd_qcmp))::NUMERIC * ((
		(cast(f552.amount_price_per_unit_qduprc as  NUMERIC(28,6))/ 10000))::NUMERIC)::NUMERIC(9,2)) AS total_revenue,
		((f552.wwe_quantity_comp_qd_qcmp)::NUMERIC * (((cast(f552.amount_price_per_unit_qduprc  as NUMERIC(28,6)) / 10000))::NUMERIC)::NUMERIC(9,2))  AS complement_revenue   
		FROM
		(((((((udl_cpg.jde_daily_merch_settlement_detail_f55m002 f552
		JOIN
		udl_cpg.jde_daily_merch_settlement_header_f55m001 f551
		ON
		((upper(ltrim(RTRIM((f551.costcenter_qamcu)))) = upper(ltrim(RTRIM((f552.cost_center_qdmcu)))))) and 
		f552.as_on_date = current_date and f551.as_on_date = current_date
		)
		JOIN
		udl_cpg.jde_daily_business_unit_master_f0006 f6
		ON
		((upper(ltrim(RTRIM((f6.costcenter_mcmcu)))) = upper(ltrim(RTRIM((f552.cost_center_qdmcu)))))) and 
		f552.as_on_date = current_date and f6.as_on_date = current_date
		)
		JOIN
		udl_cpg.jde_daily_item_cost_f4105 f45
		ON
		((((upper(RTRIM(ltrim((f45.cost_center_comcu)))) = upper(ltrim(RTRIM((f552.cost_center_qdmcu)))))
        AND ((f45.identifier_2nd_item_colitm) = (f552.identifier_2nd_item_qdlitm))) AND ((f45.cost_method_coledg) = '07'))) 
		and f552.as_on_date = current_date and f45.as_on_date = current_date
        )
		JOIN
		udl_cpg.jde_daily_item_master_f4101 f41
		ON
		(((f41.identifier_2nd_item_imlitm) = (f552.identifier_2nd_item_qdlitm))) and 
		f552.as_on_date = current_date and f41.as_on_date = current_date
		)
		JOIN
		hive_udl_pii.restricted_jde_daily_address_by_date_f0116 f16
		ON
		((f16.address_number_alan8 = f6.addressnumber_mcan8)) and 
		f16.as_on_date = current_date and f6.as_on_date = current_date
		)
		JOIN
		udl_cpg.jde_daily_user_defined_codes_f0005 f5
		ON
		((((ltrim(RTRIM((f5.user_defined_code_drky))) = substring((f552.identifier_2nd_item_qdlitm), 1, 3))
		AND ((f5.product_code_drsy) = '41')) AND ((f5.user_defined_codes_drrt) = 'S1'))) and 
		f552.as_on_date = current_date and f5.as_on_date = current_date
		)
		JOIN
		udl_cpg.jde_daily_user_defined_codes_f0005 f15
		ON
		((((ltrim(RTRIM((f15.user_defined_code_drky))) = (f41.sales_category_code_2_imsrp2)) 
		AND ((f15.product_code_drsy) = '41')) AND ((f15.user_defined_codes_drrt) = 'S2'))) and 
		f15.as_on_date = current_date and f41.as_on_date = current_date
		)
		JOIN (select * from fds_cpg.dim_cpg_item where dim_business_unit_id in 
		(select dim_business_unit_id from fds_cpg.dim_cpg_business_unit
		where src_business_unit_id='W03')) di
		ON di.src_item_id = upper(LTRIM(RTRIM(f41.identifier_2nd_item_imlitm))) and upper(di.active_flag) = 'Y'
		JOIN cdm.dim_event de  
		ON upper(LTRIM(RTRIM(de.event_src_sys_id))) = upper(LTRIM(RTRIM(f6.costcenter_mcmcu)))
		inner join
		fds_le.brdg_live_event_venue v on v.dim_event_id = de.dim_event_id
		) A
		GROUP BY 
        A.time_key,
		A.Item_key,
		A.event_key,
		A.venue_key ;
		SELECT DISTINCT   CAST(to_char(de.event_dttm,'YYYYMMDD')as bigint) time_key,
        upper(ltrim(rtrim(di.src_item_id))) as Item_key,
        de.dim_event_id event_key,
        v.dim_venue_id as venue_key,  
		0 AS quantity_shipped ,
		0 as quantity_adjustment,
		0 AS quantity_returned, 
		0 AS Compelements ,
		SUM(tab1.Qty_Sold) as net_units_sold, 
		SUM(Tab1.Selling_price) as selling_price,
		SUM(tab1.amount) as total_revenue ,
		0 as complement_revenue ,
		'C' as flag
		INTO #TEMP_C
		FROM 
		(Select cost_center_header_sdemcu Event, 
		cast(cast(price_per_unit_amount_sduprc as  NUMERIC(28,6))/10000 as dec(13,4)) Selling_price, 
		shipped_units_quantity_sdsoqs Qty_Sold, item_number_2nd_sdlitm Item, description_line_1_sddsc1 Item_Description, cast(cast(extended_price_amount_sdaexp as NUMERIC(28,6))/100 as dec(13,2)) Amount 
		from udl_cpg.jde_daily_sales_order_history_f42119 
		where status_code_last_sdlttr < '980' 
		and trim(cost_center_header_sdemcu) in 
		(select trim(costcenter_mcmcu) 
		from udl_cpg.jde_daily_business_unit_master_f0006 
		where costcentertype_mcstyl = 'EV' and as_on_date = current_date
		)
		and item_number_2nd_sdlitm in 
		(select identifier_2nd_item_imlitm 
		from udl_cpg.jde_daily_item_master_f4101 
		where description_line_1_imdsc1 like '%Cup%' or description_line_1_imdsc1 like '%CUP%' 
		and as_on_date = current_date
		)
		AND as_on_date = current_date
		)Tab1
		INNER JOIN
		(select * from fds_cpg.dim_cpg_item 
		where dim_business_unit_id in 
		(select dim_business_unit_id 
		from fds_cpg.dim_cpg_business_unit
		where src_business_unit_id='W03')) di
		ON upper(ltrim(rtrim(di.src_item_id))) = upper(ltrim(rtrim(tab1.item)))
		and upper(di.active_flag) = 'Y'
		JOIN cdm.dim_event de  
		ON upper(ltrim(rtrim(tab1.event))) = upper(ltrim(rtrim(de.event_src_sys_id)))
		inner join
		fds_le.brdg_live_event_venue v on v.dim_event_id=de.dim_event_id
		GROUP BY de.event_dttm,di.src_item_id,de.dim_event_id,v.dim_venue_id,quantity_shipped,
		quantity_adjustment,quantity_returned,Compelements,complement_revenue ;
		DELETE FROM #TEMP_V WHERE (Event_key,Item_key) in (SELECT EVENT_KEY,ITEM_KEY FROM #TEMP_C )  ;
		SELECT * INTO #TEMP_R FROM 
		(
		SELECT  
		time_key,
        Item_key,
        event_key,
        venue_key,
	    quantity_shipped,
	    quantity_adjustment  ,
	    quantity_returned,
	    compelements ,
	    net_units_sold ,
	    Selling_price,
	    total_revenue,
		complement_revenue 
		FROM #TEMP_V
		UNION ALL
		SELECT  
		time_key,
        Item_key,
        event_key,
        venue_key,
	    quantity_shipped,
	    quantity_adjustment  ,
	    quantity_returned,
	    compelements ,
	    net_units_sold ,
	    Selling_price,
	    total_revenue,
		complement_revenue 
		FROM #TEMP_C ) ;
		drop table if exists AGG_SALES;
		create TEMP table AGG_SALES AS (
		SELECT DISTINCT  
		time_key as date_id,
        Item_key as src_item_id,
        event_key as dim_event_id,
        venue_key as dim_venue_id,
	    cast(quantity_shipped as integer) quantity_shipped ,
	    cast(quantity_adjustment as integer)  quantity_adjustment,
	    cast(quantity_returned as integer) quantity_returned ,
	    cast(compelements as integer) compelements,
	    cast(net_units_sold as integer) net_units_sold,
	    cast(Selling_price as NUMERIC(28,6))  Selling_price,
	    round(total_revenue) total_revenue,
		complement_revenue FROM  #TEMP_R
		EXCEPT 
		SELECT 
		date_id,
		upper(ltrim(rtrim(b.src_item_id))) as src_item_id,
		dim_event_id,
		dim_venue_id,
		quantity_shipped,
		quantity_adjustment,
		quantity_returned,
		compelements,
		net_units_sold,
		cast(Selling_price as NUMERIC(28,6))  selling_price,
		round(total_revenue) total_revenue,
		complement_revenue
		FROM
		fds_cpg.aggr_cpg_daily_venue_sales a left join
		fds_cpg.dim_cpg_item b on a.dim_item_id=b.dim_item_id where lower(a.active_flag)='y') ;
		drop table if exists #stg_fact_cpg_aggregated_venue_sales;
		create table #stg_fact_cpg_aggregated_venue_sales as 
		select *,case when dim_agg_sales_id is null then 'i' else 'u' end as ins_upd_flag
		from (
		SELECT  
		T.date_id as date_id,
        I.dim_item_id as dim_item_id,
        T.dim_event_id as dim_event_id,
        T.dim_venue_id as dim_venue_id,
        T.quantity_shipped,
        T.quantity_adjustment,
        T.quantity_returned,
        T.compelements,
        T.net_units_sold,
        T.selling_price,
        T.total_revenue,
        T.complement_revenue,
        FT.dim_agg_sales_id,
		'' as selling_ratio
        FROM AGG_SALES T
		LEFT OUTER JOIN 
		(select upper(ltrim(rtrim(b.src_item_id))) src_item_id,a.* 
		from 
		fds_cpg.aggr_cpg_daily_venue_sales a left join
		fds_cpg.dim_cpg_item b on a.dim_item_id=b.dim_item_id 
		where lower(a.active_flag)='y') FT
		ON FT.src_item_id = T.src_item_id AND FT.dim_event_id = T.dim_event_id
		AND FT.dim_venue_id = T.dim_venue_id and  FT.date_id = T.date_id 
		LEFT OUTER JOIN 
		(select * from fds_cpg.dim_cpg_item where dim_business_unit_id in 
		(select dim_business_unit_id from fds_cpg.dim_cpg_business_unit
		where src_business_unit_id='W03')  and upper(active_flag) = 'Y') I
		ON upper(ltrim(rtrim(I.src_item_id))) = upper(ltrim(rtrim(T.src_item_id))))  ;
		UPDATE fds_cpg.aggr_cpg_daily_venue_sales
		SET active_flag = 'N',
		effective_end_datetime = sysdate,
		etl_update_rec_dttm = sysdate,
		etl_update_user_id = 'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_4B'
		FROM 
		fds_cpg.aggr_cpg_daily_venue_sales df
		INNER JOIN 
		#stg_fact_cpg_aggregated_venue_sales sf
		on sf.dim_agg_sales_id = df.dim_agg_sales_id and lower(df.active_flag) = 'y'  ;
		"],
		"materialized": 'incremental','tags': "Phase 5B"
  })
}}
SELECT
    (select max(dim_agg_sales_id) from {{this}}) + row_number() OVER () as dim_agg_sales_id,
    date_id,
    dim_item_id,
    dim_event_id,
    dim_venue_id,
    quantity_shipped,
    quantity_adjustment,
    quantity_returned,
    compelements,
    net_units_sold,
    selling_price,
    total_revenue,
    complement_revenue,
    'Y' as active_flag,
	current_timestamp as effective_start_datetime,
    cast('2050-12-31 00:00:00' as timestamp) as effective_end_datetime,
	50001 as etl_batch_id, 
	'bi_dbt_user_prd' as etl_insert_user_id, 
	current_timestamp as etl_insert_rec_dttm, 
	null as etl_update_user_id, 
	cast(null as timestamp) as etl_update_rec_dttm
FROM
	(select distinct date_id,dim_item_id,dim_event_id,dim_venue_id,
	quantity_shipped,quantity_adjustment,quantity_returned,compelements,
	net_units_sold,selling_price,total_revenue,complement_revenue,
	selling_ratio,ins_upd_flag 
	from #stg_fact_cpg_aggregated_venue_sales
	WHERE lower(ins_upd_flag) in  ('i','u'))