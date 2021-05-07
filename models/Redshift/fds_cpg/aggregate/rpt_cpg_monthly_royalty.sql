{{
  config({
		'schema': 'fds_cpg',
		"pre-hook": ["delete from fds_cpg.rpt_cpg_monthly_royalty"],
		"materialized": 'incremental','tags': "Phase 5B","post-hook" : 'grant select on {{this}} to public'
  })
}}


with dataset_clientbase_shipments as
(
	select distinct 
			'CLIENTBASE' AS source
			, currency_type
			, report_month
			, item_id
			, abs(COALESCE(SUM(qty_shipped),0)) AS shipped_quantity
			, abs(COALESCE(SUM(net_merchandise_exc_vat),0.000)) AS shipped_sales
	from {{source('udl_pii','restricted_cpg_clientbase_monthly_shipment')}}
	group by 1,2,3,4
),

dataset_clientbase_returns as
(
	select distinct 
			'CLIENTBASE' AS source
			, currency_type
			, report_month
			, item_id
			, abs(COALESCE(count(*),0)) AS returned_quantity
			, abs(COALESCE(SUM(merch_net),0.000)) AS returned_sales
	from {{source('hive_udl_cpg','clientbase_monthly_returns')}}
	group by 1,2,3,4
),

dataset_radial_shipments as
(
	select distinct 
			'RADIAL' AS source
			, currency_type
			, report_month
			, item_id
			, abs(COALESCE(SUM(units_shipped),0))  AS shipped_quantity
			, abs(COALESCE(SUM(merch_net),0.000)) AS shipped_sales
	from {{source('hive_udl_cpg','radial_monthly_order_ship')}}
	group by 1,2,3,4

),

dataset_radial_returns as
(

	select distinct 
			'RADIAL' AS source
			, currency_type
			, report_month
			, item_id
			, abs(COALESCE(SUM(units_returned),0))  AS returned_quantity
			, abs(COALESCE(SUM(merch_net),0.000)) AS returned_sales
	from {{source('hive_udl_cpg','radial_monthly_order_return')}}
	group by 1,2,3,4
),

dataset_radial_refunds as
(

	select distinct 
			'RADIAL' AS source
			, 'USD' as currency_type
			, report_month
			, sku as item_id
			, 0 as refunded_quantity
			, abs(COALESCE(SUM(product_amount),0.00)) AS refunded_sales
	from {{source('hive_udl_cpg','radial_monthly_payment_refund')}}
	where return1_return2=1
	group by 1,2,3,4
),


dataset_amazon_payments as
(

	select distinct 
			upper(source) as source
			, upper(currency_code) as currency_type
			, to_date(as_on_date,'YYYYMMDD') as report_month
			, sku as item_id
			, abs(COALESCE(count(distinct quantity),0))  AS shipped_quantity
			, abs(COALESCE(SUM(case when type_english='adjustment' then other else (product_sales+promotional_rebates) end),0.000)) AS shipped_sales
	from {{source('hive_udl_cpg','raw_amazon_monthly_payments')}}
	where lower(type_english) in ('order','adjustment')
	group by 1,2,3,4
),

dataset_amazon_refunds as
(

	select distinct 
			upper(source) as source
			, upper(currency_code) as currency_type
			, to_date(as_on_date,'YYYYMMDD') as report_month
			, sku as item_id
			, abs(COALESCE(count(distinct quantity),0))  AS refunded_quantity
			, abs(COALESCE(SUM(product_sales+promotional_rebates),0.000)) AS refunded_sales
	from {{source('hive_udl_cpg','raw_amazon_monthly_payments')}}
	where lower(type_english) in ('refund')
	group by 1,2,3,4
),


dataset_royalty_report as
(
	------ Clientbase Royalty Report
	select (case when a.source is null then b.source else a.source end) as source
		,(case when a.currency_type is null then b.currency_type else a.currency_type end) as currency_type
		,(case when a.report_month is null then b.report_month else a.report_month end) as report_month
		,(case when a.item_id is null then b.item_id else a.item_id end) as item_id 
		,coalesce(a.shipped_quantity, 0) as shipped_quantity
		,coalesce(a.shipped_sales, 0.000) as shipped_sales
		,coalesce(b.returned_quantity,0) as returned_quantity
		,coalesce(b.returned_sales, 0.000) as returned_sales
		,0 as refunded_quantity
		,0.000 as refunded_sales
	from 
	dataset_clientbase_shipments a
	full join dataset_clientbase_returns b on a.source=b.source and a.currency_type=b.currency_type and a.report_month=b.report_month and a.item_id=b.item_id

	UNION

	------ Radial Royalty Report
	select (case when a.source is null then b.source else a.source end) as source
		,(case when a.currency_type is null then b.currency_type else a.currency_type end) as currency_type
		,(case when a.report_month is null then b.report_month else a.report_month end) as report_month
		,(case when a.item_id is null then b.item_id else a.item_id end) as item_id 
		,coalesce(a.shipped_quantity, 0) as shipped_quantity
		,coalesce(a.shipped_sales, 0.000) as shipped_sales
		,coalesce(a.returned_quantity,0) as returned_quantity
		,coalesce(a.returned_sales, 0.000) as returned_sales
		,coalesce(b.refunded_quantity,0) as refunded_quantity
		,coalesce(b.refunded_sales, 0.000) as refunded_sales
	from 
	(
		select (case when a.source is null then b.source else a.source end) as source
                        ,(case when a.currency_type is null then b.currency_type else a.currency_type end) as currency_type
                        ,(case when a.report_month is null then b.report_month else a.report_month end) as report_month
                        ,(case when a.item_id is null then b.item_id else a.item_id end) as item_id 
                        ,coalesce(a.shipped_quantity, 0) as shipped_quantity
                        ,coalesce(a.shipped_sales, 0.000) as shipped_sales
                        ,coalesce(b.returned_quantity,0) as returned_quantity
                        ,coalesce(b.returned_sales, 0.000) as returned_sales
	       from 
	       dataset_radial_shipments a
	       full join dataset_radial_returns b on a.source=b.source and a.currency_type=b.currency_type and a.report_month=b.report_month and a.item_id=b.item_id
	) a
	full join dataset_radial_refunds b on a.source=b.source and a.currency_type=b.currency_type and a.report_month=b.report_month and a.item_id=b.item_id
	
	UNION ALL
	
	------ Amazon Royalty reports 
	select (case when a.source is null then b.source else a.source end) as source
		,(case when a.currency_type is null then b.currency_type else a.currency_type end) as currency_type
		,(case when a.report_month is null then b.report_month else a.report_month end) as report_month
		,(case when a.item_id is null then b.item_id else a.item_id end) as item_id 
		,coalesce(a.shipped_quantity, 0) as shipped_quantity
		,coalesce(a.shipped_sales, 0.000) as shipped_sales
		,0  as returned_quantity
		,0.000 as returned_sales
		,coalesce(b.refunded_quantity,0) as refunded_quantity
		,coalesce(b.refunded_sales, 0.000) as refunded_sales
	from 
	dataset_amazon_payments a
	full join dataset_amazon_refunds b on a.source=b.source and a.currency_type=b.currency_type and a.report_month=b.report_month and a.item_id=b.item_id
	
),

dataset_dim_item as
(
select * from 
(
        select distinct src_item_id as item_id, src_item_description as item_description, src_royalty_name as royalty_code, 
        row_number() over (partition by src_item_id order by effective_start_datetime desc) as row_num
        from {{source('fds_cpg','dim_cpg_item')}} where active_flag='Y'
) where row_num=1
)

select 
a.source
,a.currency_type
,a.report_month
,initcap(c.mth_nm) AS "month"
,c.cal_year as year
,UPPER(c.cal_year_qtr_desc) AS quarter
,a.item_id
,b.item_description
,b.royalty_code
,a.shipped_quantity
,a.shipped_sales
,(-a.returned_quantity) as returned_quantity
,(-a.returned_sales) as returned_sales
,(-a.refunded_quantity) as refunded_quantity
,(-a.refunded_sales) as refunded_sales ,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_FDS_CPG' as etl_batch_id, 'bi_dbt_user_prd' as etl_insert_user_id, 
current_timestamp as etl_insert_rec_dttm, null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from dataset_royalty_report a
left outer join dataset_dim_item b on a.item_id=b.item_id
inner join {{source('cdm','dim_date')}} c on a.report_month=c.full_date