{{
  config({
		"materialized": 'ephemeral'
  })
}}
Select dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
      ,dim_order_method_id  
      ,src_order_type   
      ,Date_Key  
      ,dim_item_id 
      ,sum(Other_Amount) as Other_Amount 
      ,0 as src_unit_cost  
      ,0 as src_current_retail_price  
      ,0 as src_units_ordered  
      ,0 as src_units_shipped  
      ,0 as Units_Returned  
      ,0 as Net_Units_Sold  
      ,0 as "Demand_Cogs$"  
      ,0 as "Shipped_Cogs$"  
      ,0 as "Returned_Cogs$"  
      ,0 as "Net_Cogs$"  
      ,0 as "Demand_Retail$"  
      ,0 as "Shipped_Retail$"  
      ,0 as "Net_Retail$"  
      ,0 as "Demand_Sales$"  
      ,0 as "Shipped_Sales$"  
      ,0 as "Return$"  
      ,0 as "Net_sales_$"  
      ,0 as "Demand_Selling_Margin$"  
      ,0 as "Shipped_Selling_Margin$"  
      ,0 as "Net_Selling_Margin$" 
from 
(SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
     ,dim_order_method_id  
	 ,src_order_type         
     ,order_date_id as Date_Key  
     ,(select dim_item_id 
		from {{source('fds_cpg','dim_cpg_item')}} 
		where src_item_id='TAX' and active_flag='Y') AS dim_item_id  
     ,Sum(src_sales_tax) Other_Amount  
     ,0 as SPECIALCHG  
FROM {{source('fds_cpg','fact_cpg_sales_header')}} 
		left join 
		(select src_channel_id,dim_order_method_id as dm_order_method_id 
		from {{source('fds_cpg','dim_cpg_order_method')}}) dim_cpg_order_method
		on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dm_order_method_id 
	where  order_date_id>=0 and src_order_type='I' and src_channel_id<>'R'  
			and (isnull(src_order_origin_code,'AA')<>'GR' or isnull(src_prepay_code,'A')<>'F')  
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,order_date_id  
Union All  
SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id  
     ,case src_order_type when 'I'  then 'G' else 'G' end src_order_type         
     ,order_date_id as Date_Key  
     ,(select dim_item_id from {{source('fds_cpg','dim_cpg_item')}} 
		where src_item_id='TAX' and active_flag='Y') AS dim_item_id  
     ,Sum(src_sales_tax) Other_Amount  
     ,0 as SPECIALCHG  
FROM {{source('fds_cpg','fact_cpg_sales_header')}}
	left join 
	(select src_channel_id,dim_order_method_id as dm_order_method_id 
	from {{source('fds_cpg','dim_cpg_order_method')}}) dim_cpg_order_method
	on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dm_order_method_id 
	where order_date_id>=0 and src_order_type='I' and src_channel_id<>'R' and isnull(src_order_origin_code,'AA')='GR'  
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, case src_order_type when 'I'  then 'G' else 'G' end ,order_date_id  
Union All  
SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
     ,dim_order_method_id  
     ,case src_order_type when 'I'  then 'F' else 'F' end src_order_type  
     ,order_date_id as Date_Key  
     ,(select dim_item_id 
		from {{source('fds_cpg','dim_cpg_item')}} where src_item_id='TAX' and active_flag='Y') AS dim_item_id  
     ,Sum(src_sales_tax) Other_Amount  
     ,0 as SPECIALCHG  
FROM {{source('fds_cpg','fact_cpg_sales_header')}} 
    left join 
	(select src_channel_id,dim_order_method_id as dm_order_method_id 
	from {{source('fds_cpg','dim_cpg_order_method')}}) dim_cpg_order_method
	on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dm_order_method_id 
	where order_date_id>=0 and src_order_type='I' and src_channel_id<>'R' and (isnull(src_prepay_code,'A')='F' 
			and isnull(src_order_origin_code,'AA')<>'GR')  
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, case src_order_type when 'I'  then 'F' else 'F' end ,order_date_id  
Union All  
SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
     ,dim_order_method_id  
     ,src_order_type  
     ,order_date_id as Date_Key  
     ,(select dim_item_id 
	 from {{source('fds_cpg','dim_cpg_item')}} 
	 where src_item_id='TAX' and active_flag='Y') AS dim_item_id  
     ,Sum(src_sales_tax) Other_Amount  
     ,0 as SPECIALCHG  
FROM {{source('fds_cpg','fact_cpg_sales_header')}} 
	left join 
	(select src_channel_id,dim_order_method_id as dm_order_method_id 
	from {{source('fds_cpg','dim_cpg_order_method')}}) dim_cpg_order_method
	on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dm_order_method_id 
	where  order_date_id>=0 and src_channel_id='R'   
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,order_date_id  
    ) Tab_Tax  
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id,src_order_type,Date_Key,dim_item_id   
Union All  
---Spceial Charges-----  
Select dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
      ,dim_order_method_id  
      ,src_order_type   
      ,Date_Key  
      ,dim_item_id  
      ,sum(Other_Amount) as Other_Amount  
      ,0 as src_unit_cost  
      ,0 as src_current_retail_price  
      ,0 as src_units_ordered  
      ,0 as src_units_shipped  
      ,0 as Units_Returned  
      ,0 as Net_Units_Sold  
      ,0 as "Demand_Cogs$"  
      ,0 as "Shipped_Cogs$"  
      ,0 as "Returned_Cogs$"  
      ,0 as "Net_Cogs$"  
      ,0 as "Demand_Retail$"  
      ,0 as "Shipped_Retail$"  
      ,0 as "Net_Retail$"  
      ,0 as "Demand_Sales$"  
      ,0 as "Shipped_Sales$"  
      ,0 as "Return$"  
      ,0 as "Net_sales_$"  
      ,0 as "Demand_Selling_Margin$"  
      ,0 as "Shipped_Selling_Margin$"  
      ,0 as "Net_Selling_Margin$" 
from       
   (SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
     ,dim_order_method_id  
     ,src_order_type         
     ,order_date_id as Date_Key  
     ,(select dim_item_id 
	 from {{source('fds_cpg','dim_cpg_item')}} 
	 where src_item_id='SPECIALCHG' and active_flag='Y') AS dim_item_id  
     ,SUM(src_special_charges) Other_Amount  
     ,0 as SPECIALCHG  
    FROM {{source('fds_cpg','fact_cpg_sales_header')}} 
		left join 
		(select src_channel_id,dim_order_method_id as dm_order_method_id 
		from {{source('fds_cpg','dim_cpg_order_method')}}) dim_cpg_order_method
		on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dm_order_method_id 
	where order_date_id>=0 and src_order_type='I' and src_channel_id<>'R'  
		and (isnull(src_order_origin_code,'AA')<>'GR' or isnull(src_prepay_code,'A')<>'F')  
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,order_date_id  
Union All  
SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
     ,dim_order_method_id  
     ,case src_order_type when 'I'  then 'G' else 'G' end src_order_type         
     ,order_date_id as Date_Key  
    ,(select dim_item_id 
	from {{source('fds_cpg','dim_cpg_item')}} 
	where src_item_id='SPECIALCHG' and active_flag='Y') AS dim_item_id  
    ,SUM(src_special_charges) Other_Amount  
    ,0 as SPECIALCHG  
FROM {{source('fds_cpg','fact_cpg_sales_header')}} 
	left join 
	(select src_channel_id,dim_order_method_id as dm_order_method_id 
	from {{source('fds_cpg','dim_cpg_order_method')}}) dim_cpg_order_method
	on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dm_order_method_id 
where order_date_id>=0 and src_order_type='I' and src_channel_id<>'R' and isnull(src_order_origin_code,'AA')='GR'  
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, case src_order_type when 'I'  then 'G' else 'G' end ,order_date_id  
Union All  
SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
     ,dim_order_method_id  
     ,case src_order_type when 'I'  then 'F' else 'F' end src_order_type  
     ,order_date_id as Date_Key  
     ,(select dim_item_id 
	 from {{source('fds_cpg','dim_cpg_item')}} 
	 where src_item_id='SPECIALCHG' and active_flag='Y') AS dim_item_id  
    ,SUM(src_special_charges) Other_Amount  
    ,0 as SPECIALCHG  
FROM {{source('fds_cpg','fact_cpg_sales_header')}}
	left join 
	(select src_channel_id,dim_order_method_id as dm_order_method_id 
	from {{source('fds_cpg','dim_cpg_order_method')}}) dim_cpg_order_method
	on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dm_order_method_id 
where order_date_id>=0 and src_order_type='I' and src_channel_id<>'R'  
    and (isnull(src_prepay_code,'A')='F' and isnull(src_order_origin_code,'AA')<>'GR')  
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, case src_order_type when 'I'  then 'F' else 'F' end ,order_date_id  
Union All  
SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
     ,dim_order_method_id  
     ,src_order_type  
     ,order_date_id as Date_Key  
     ,(select dim_item_id 
	 from {{source('fds_cpg','dim_cpg_item')}} 
	 where src_item_id='SPECIALCHG' and active_flag='Y') AS dim_item_id  
    ,SUM(src_special_charges) Other_Amount  
    ,0 as SPECIALCHG  
FROM {{source('fds_cpg','fact_cpg_sales_header')}} 
	left join 
	(select src_channel_id,dim_order_method_id as dm_order_method_id 
	from {{source('fds_cpg','dim_cpg_order_method')}}) dim_cpg_order_method
	on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dm_order_method_id 
where order_date_id>=0 and src_channel_id='R'   
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,order_date_id) Tab_Tax  
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id,src_order_type,Date_Key,dim_item_id   
Union All  
---- Freight ----------  
Select dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
      ,dim_order_method_id  
      ,src_order_type   
      ,Date_Key  
      ,dim_item_id  
      ,sum(Other_Amount) as Other_Amount
      ,0 as src_unit_cost  
      ,0 as src_current_retail_price  
      ,0 as src_units_ordered  
      ,0 as src_units_shipped  
      ,0 as Units_Returned  
      ,0 as Net_Units_Sold  
      ,0 as "Demand_Cogs$"  
      ,0 as "Shipped_Cogs$"  
      ,0 as "Returned_Cogs$"  
      ,0 as "Net_Cogs$"  
      ,0 as "Demand_Retail$"  
      ,0 as "Shipped_Retail$"  
      ,0 as "Net_Retail$"  
      ,0 as "Demand_Sales$"  
      ,0 as "Shipped_Sales$"  
      ,0 as "Return$"  
      ,0 as "Net_sales_$"  
      ,0 as "Demand_Selling_Margin$"  
      ,0 as "Shipped_Selling_Margin$"  
      ,0 as "Net_Selling_Margin$"   
from       
   (SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
     ,dim_order_method_id  
     ,src_order_type         
     ,order_date_id as Date_Key  
     ,(select dim_item_id from {{source('fds_cpg','dim_cpg_item')}} 
	 where src_item_id='FREIGHT' and active_flag='Y') AS dim_item_id  
     ,Sum(src_carrier_shipping_charges) Other_Amount  
     ,0 as SPECIALCHG  
    FROM {{source('fds_cpg','fact_cpg_sales_header')}} 
		left join 
		(select src_channel_id,dim_order_method_id as dm_order_method_id 
		from {{source('fds_cpg','dim_cpg_order_method')}}) dim_cpg_order_method
		on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dm_order_method_id 
	where order_date_id>=0 and src_order_type='I' and src_channel_id<>'R'  
		and (isnull(src_order_origin_code,'AA')<>'GR' or isnull(src_prepay_code,'A')<>'F')  
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,order_date_id  
	Union All  
    SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
     ,dim_order_method_id  
    ,case src_order_type when 'I'  then 'G' else 'G' end src_order_type         
    ,order_date_id as Date_Key  
    ,(select dim_item_id 
	from {{source('fds_cpg','dim_cpg_item')}} 
	where src_item_id='FREIGHT' and active_flag='Y') AS dim_item_id  
    ,Sum(src_carrier_shipping_charges) Other_Amount  
    ,0 as SPECIALCHG  
    FROM {{source('fds_cpg','fact_cpg_sales_header')}} 
		left join 
		(select src_channel_id,dim_order_method_id as dm_order_method_id 
		from {{source('fds_cpg','dim_cpg_order_method')}}) dim_cpg_order_method
		on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dm_order_method_id 
	where order_date_id>=0 and src_order_type='I' and src_channel_id<>'R' and isnull(src_order_origin_code,'AA')='GR'  
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, case src_order_type when 'I'  then 'G' else 'G' end ,order_date_id  
	Union All  
    SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
     ,dim_order_method_id  
    ,case src_order_type when 'I'  then 'F' else 'F' end src_order_type  
    ,order_date_id as Date_Key  
    ,(select dim_item_id 
	from {{source('fds_cpg','dim_cpg_item')}} 
	where src_item_id='FREIGHT' and active_flag='Y') AS dim_item_id  
    ,Sum(src_carrier_shipping_charges) Other_Amount  
    ,0 as SPECIALCHG  
    FROM {{source('fds_cpg','fact_cpg_sales_header')}} 
		left join 
		(select src_channel_id,dim_order_method_id as dm_order_method_id 
		from {{source('fds_cpg','dim_cpg_order_method')}}) dim_cpg_order_method
		on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dm_order_method_id 
	where order_date_id>=0 and src_order_type='I' and src_channel_id<>'R'   
		and (isnull(src_prepay_code,'A')='F' and isnull(src_order_origin_code,'AA')<>'GR')  
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, case src_order_type when 'I'  then 'F' else 'F' end ,order_date_id  
   Union All  
    SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
     ,dim_order_method_id  
	 ,src_order_type  
     ,order_date_id as Date_Key  
     ,(select dim_item_id 
	 from {{source('fds_cpg','dim_cpg_item')}} 
	 where src_item_id='FREIGHT' and active_flag='Y') AS dim_item_id  
     ,Sum(src_carrier_shipping_charges) Other_Amount  
     ,0 as SPECIALCHG  
    FROM {{source('fds_cpg','fact_cpg_sales_header')}}
		left join 
		(select src_channel_id,dim_order_method_id as dm_order_method_id 
		from {{source('fds_cpg','dim_cpg_order_method')}}) dim_cpg_order_method
		on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dm_order_method_id 
	where order_date_id>=0 and src_channel_id='R'   
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,order_date_id) Tab_Tax  
group by  dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id,src_order_type,Date_Key,dim_item_id