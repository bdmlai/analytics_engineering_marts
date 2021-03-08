{{
  config({
		"materialized": 'ephemeral'
  })
}}
select dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
      ,dim_order_method_id  
      ,src_order_type  
      ,Date as Date_Key  
      ,dim_item_id  
      ,0 as Other_amount
      ,Avg(src_unit_cost) as src_unit_cost  
      ,Avg(src_current_retail_price) as src_current_retail_price  
      ,Sum(src_units_ordered) as src_units_ordered  
      ,sum(src_units_shipped) as src_units_shipped  
      ,sum(Units_Returned) as Units_Returned  
      ,sum(Net_Units_Sold) as Net_Units_Sold  
      ,sum("Demand_Cogs$") as "Demand_Cogs$"  
      ,sum ("Shipped_Cogs$") as "Shipped_Cogs$"  
      ,sum("Returned_Cogs$") as "Returned_Cogs$"  
      ,sum ("Net_Cogs$") as "Net_Cogs$"  
      ,sum("Demand_Retail$") as "Demand_Retail$"  
      ,sum ("Shipped_Retail$") as "Shipped_Retail$"  
      ,sum ("Net_Retail$") as "Net_Retail$"  
      ,sum("Demand_Sales$") as "Demand_Sales$"  
      ,sum ("Shipped_Sales$") as "Shipped_Sales$"  
      ,sum("Return$") as "Return$"  
      ,sum("Net_Sales_$") as "Net_Sales_$"  
      ,sum("Demand_Selling_Margin$") as "Demand_Selling_Margin$"  
      ,sum("Shipped_Selling_Margin$") as "Shipped_Selling_Margin$"  
      ,sum("Net_Selling_Margin$") as "Net_Selling_Margin$"  
      from   
  (       
--sub1 select  
Select dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
      ,dim_order_method_id  
      ,case src_order_type when 'C' then 'F' else 'F' end as src_order_type   
      ,Date  
      ,dim_item_id  
      ,Avg(src_unit_cost) as src_unit_cost  
      ,Avg(src_current_retail_price) as src_current_retail_price  
      ,Sum(src_units_ordered) as src_units_ordered  
      ,sum(src_units_shipped) as src_units_shipped  
      ,sum(Units_Returned) as Units_Returned  
      ,sum(src_units_shipped+ Units_Returned) as Net_Units_Sold  
      ,sum("Demand_Cogs$") as "Demand_Cogs$"  
      ,sum ("Shipped_Cogs$") as "Shipped_Cogs$"  
      ,sum("Returned_Cogs$") as "Returned_Cogs$"  
      ,sum ("Net_Cogs$") as "Net_Cogs$"  
      ,sum("Demand_Retail$") as "Demand_Retail$"  
      ,sum ("Shipped_Retail$") as "Shipped_Retail$"  
      ,sum ("Net_Retail$") as "Net_Retail$"  
      ,sum("Demand_Sales$") as "Demand_Sales$"  
      ,sum("Shipped_Sales$") as "Shipped_Sales$"  
      ,sum("Return$") as "Return$"  
      ,sum("Net_Sales_$") as "Net_Sales_$"  
      ,sum("Demand_Selling_Margin$") as "Demand_Selling_Margin$"  
      ,sum("Shipped_Selling_Margin$") as "Shipped_Selling_Margin$"  
      ,sum("Net_Selling_Margin$") as "Net_Selling_Margin$"  
	FROM  (  
/*Retrieving Demand sales for Regular Items*/  
    SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
      ,dim_order_method_id  
      ,case src_order_type when 'I' then 'F' else 'F' end as src_order_type         
      ,Date,dim_item_id  
      ,Avg(src_unit_cost) as src_unit_cost  
      ,Avg(src_current_retail_price) as src_current_retail_price  
      ,Avg(src_selling_price) as src_selling_price  
      ,sum(src_units_ordered) as src_units_ordered  
      ,SUM(src_units_shipped) as src_units_shipped  
      ,SUM(Units_Returned) as Units_Returned  
      ,sum("Demand_Cogs$") as "Demand_Cogs$"  
      ,sum ("Shipped_Cogs$") as "Shipped_Cogs$"  
      ,sum("Returned_Cogs$") as "Returned_Cogs$"  
      ,sum ("Net_Cogs$") as "Net_Cogs$"  
      ,sum("Demand_Retail$") as "Demand_Retail$"  
      ,sum ("Shipped_Retail$") as "Shipped_Retail$"  
      ,sum ("Net_Retail$") as "Net_Retail$"  
      ,sum("Demand_Sales$") as "Demand_Sales$"  
      ,sum ("Shipped_Sales$") as "Shipped_Sales$"  
      ,sum("Return$") as "Return$"  
      ,sum("Net_Sales_$") as "Net_Sales_$"  
      ,sum(isnull("Demand_Sales$",0)-isnull("Demand_Cogs$",0)) as "Demand_Selling_Margin$"  
      ,sum(isnull("Shipped_Sales$",0)-isnull("Shipped_Cogs$",0)) as "Shipped_Selling_Margin$"  
      ,sum(isnull("Net_Sales_$",0)-isnull("Net_Cogs$",0)) as "Net_Selling_Margin$"  
    from (SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
      ,fact_cpg_sales_detail.dim_order_method_id  
      ,src_order_type,order_date_id as Date,dim_item_id  
      ,Avg(src_unit_cost) as src_unit_cost  
      ,Avg(src_current_retail_price) as src_current_retail_price  
      ,Avg(src_selling_price) as src_selling_price  
      ,sum(src_units_ordered) as src_units_ordered  
      ,0 as src_units_shipped  
      ,0 as Units_Returned  
      ,sum(src_units_ordered *isnull(src_unit_cost,0)) as "Demand_Cogs$"  
      ,0 as "Shipped_Cogs$"  
      ,0 as "Returned_Cogs$"  
      ,0 as "Net_Cogs$"  
      ,sum(src_units_ordered * isnull(src_current_retail_price,0)) as "Demand_Retail$"  
      ,0 as "Shipped_Retail$"  
      ,0 as "Net_Retail$"  
      ,sum(src_units_ordered*isnull(src_selling_price,0)) as "Demand_Sales$"  
      ,0 as "Shipped_Sales$"  
      ,0 as "Return$"  
      ,0 as "Net_Sales_$"  
    FROM {{source('fds_cpg','fact_cpg_sales_detail')}}  
        left join {{source('fds_cpg','dim_cpg_order_method')}} 
		on fact_cpg_sales_detail.dim_order_method_id= dim_cpg_order_method.dim_order_method_id   
	where (order_date_id>=0 or ship_date_id>=0) and   
            dim_item_id  not in (select distinct dim_item_id 
								from {{source('fds_cpg','dim_cpg_item')}} 
								where src_item_id in ('990-000-002-0','990-000-003-0')) and  
            src_order_type='I' and src_channel_id<>'R' and  
            dim_item_id not in (select distinct B.dim_item_id  
								from (select distinct B.dim_item_id as kit_id 
										from {{source('fds_cpg','dim_cpg_kit_item')}} A 
											inner join {{source('fds_cpg','dim_cpg_item')}} B  
											on A.src_kit_id=B.src_item_id) A, {{source('fds_cpg','dim_cpg_item')}} B 
								where A.kit_id=B.dim_item_id )
            and src_order_number  in (SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number  
										FROM {{source('fds_cpg','fact_cpg_sales_header')}} 
										where ltrim(rtrim(src_prepay_code))='F' and isnull(ltrim(rtrim(src_order_origin_code)),'AA')<>'GR')  
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,fact_cpg_sales_detail.dim_order_method_id, src_order_type ,order_date_id,dim_item_id) Tab1  
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id             
    UNION all  
/*Retrieving Shipped sales for Regular Items*/           
SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
      ,dim_order_method_id  
      ,case src_order_type when 'I' then 'F' else 'F' end as src_order_type         
      ,Date,dim_item_id  
      ,Avg(src_unit_cost) as src_unit_cost  
      ,Avg(src_current_retail_price) as src_current_retail_price  
      ,Avg(src_selling_price) as src_selling_price  
      ,sum(src_units_ordered) as src_units_ordered  
      ,SUM(src_units_shipped) as src_units_shipped  
      ,sum(Units_Returned) as Units_Returned  
      ,sum("Demand_Cogs$") as "Demand_Cogs$"  
      ,sum ("Shipped_Cogs$") as "Shipped_Cogs$"  
      ,sum("Returned_Cogs$") as "Returned_Cogs$"  
      ,sum ("Net_Cogs$") as "Net_Cogs$"  
      ,sum("Demand_Retail$") as "Demand_Retail$"  
      ,sum ("Shipped_Retail$") as "Shipped_Retail$"  
      ,sum ("Net_Retail$") as "Net_Retail$"  
      ,sum("Demand_Sales$") as "Demand_Sales$"  
      ,sum ("Shipped_Sales$") as "Shipped_Sales$"  
      ,sum("Return$") as "Return$"  
      ,sum("Net_Sales_$") as "Net_Sales_$"  
      ,sum(isnull("Demand_Sales$",0)-isnull("Demand_Cogs$",0)) as "Demand_Selling_Margin$"  
      ,sum(isnull("Shipped_Sales$",0)-isnull("Shipped_Cogs$",0)) as "Shipped_Selling_Margin$"  
      ,sum(isnull("Net_Sales_$",0)-isnull("Net_Cogs$",0)) as "Net_Selling_Margin$"  
        from (SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
      ,fact_cpg_sales_detail.dim_order_method_id  
      ,src_order_type,ship_date_id as Date,dim_item_id                      
      ,Avg(src_unit_cost) as src_unit_cost  
      ,Avg(src_current_retail_price) as src_current_retail_price  
      ,Avg(src_selling_price) as src_selling_price  
      ,0 as src_units_ordered  
      ,sum(src_units_shipped) as src_units_shipped  
      ,0 as Units_Returned  
      ,0 as "Demand_Cogs$"  
      ,sum (src_units_shipped*isnull(src_unit_cost,0)) as "Shipped_Cogs$"  
      ,0 as "Returned_Cogs$"  
      ,sum (src_units_shipped   *isnull(src_unit_cost,0)) as "Net_Cogs$"  
      ,0 as "Demand_Retail$"  
      ,sum (src_units_shipped* isnull(src_current_retail_price,0)) as "Shipped_Retail$"  
      ,sum ((src_units_shipped) *isnull(src_current_retail_price,0)) as "Net_Retail$"  
      ,0 as "Demand_Sales$"  
      ,sum (src_units_shipped*isnull(src_selling_price,0)) as "Shipped_Sales$"  
      ,0 as "Return$"  
      ,sum(src_units_shipped* isnull(src_selling_price,0)) as "Net_Sales_$"  
		FROM {{source('fds_cpg','fact_cpg_sales_detail')}}  
			left join {{source('fds_cpg','dim_cpg_order_method')}}
			on fact_cpg_sales_detail.dim_order_method_id= dim_cpg_order_method.dim_order_method_id 			
		where  (order_date_id>=0 or ship_date_id>=0) and   
				dim_item_id  not in (select distinct dim_item_id 
									from {{source('fds_cpg','dim_cpg_item')}} 
									where src_item_id in ('990-000-002-0','990-000-003-0')) and  
                src_order_type='I' and ship_date_id is not null and src_channel_id<>'R' and  
                dim_item_id not in (select distinct B.dim_item_id  from 
									(select distinct B.dim_item_id as kit_id 
									from {{source('fds_cpg','dim_cpg_kit_item')}} A 
										inner join {{source('fds_cpg','dim_cpg_item')}} B  
										on A.src_kit_id=B.src_item_id) A, {{source('fds_cpg','dim_cpg_item')}} B 
									where A.kit_id=B.dim_item_id )
                and src_order_number  in (SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number  
											FROM {{source('fds_cpg','fact_cpg_sales_header')}} 
											where ltrim(rtrim(src_prepay_code))='F' and isnull(ltrim(rtrim(src_order_origin_code)),'AA')<>'GR')  
                and src_order_status='IN'  
	group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,fact_cpg_sales_detail.dim_order_method_id, src_order_type ,ship_date_id,dim_item_id) Tab2  
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id  
    UNION ALL  
/*Retrieving Return sales for Regular Items*/             
SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
      ,dim_order_method_id  
      ,case src_order_type when 'C' then 'F' else 'F' end as src_order_type        
      ,Date,dim_item_id  
      ,Avg(src_unit_cost) as src_unit_cost  
      ,Avg(src_current_retail_price) as src_current_retail_price  
      ,Avg(src_selling_price) as src_selling_price  
      ,sum(src_units_ordered) as src_units_ordered  
      ,SUM(src_units_shipped) as src_units_shipped  
      ,SUM(Units_Returned) as Units_Returned  
      ,sum("Demand_Cogs$") as "Demand_Cogs$"  
      ,sum ("Shipped_Cogs$") as "Shipped_Cogs$"  
      ,sum("Returned_Cogs$") as "Returned_Cogs$"  
      ,sum ("Net_Cogs$") as "Net_Cogs$"  
      ,sum("Demand_Retail$") as "Demand_Retail$"  
      ,sum ("Shipped_Retail$") as "Shipped_Retail$"  
      ,sum ("Net_Retail$") as "Net_Retail$"  
      ,sum("Demand_Sales$") as "Demand_Sales$"  
      ,sum ("Shipped_Sales$") as "Shipped_Sales$"  
      ,sum("Return$") as "Return$"  
      ,sum("Net_Sales_$") as "Net_Sales_$"  
      ,sum(isnull("Demand_Sales$",0)-isnull("Demand_Cogs$",0)) as "Demand_Selling_Margin$"  
      ,sum(isnull("Shipped_Sales$",0)-isnull("Shipped_Cogs$",0)) as "Shipped_Selling_Margin$"  
      ,sum(isnull("Net_Sales_$",0)-isnull("Net_Cogs$",0)) as "Net_Selling_Margin$"  
        from (SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
      ,b.dim_order_method_id as dim_order_method_id  
      ,case src_order_type when 'C' then 'F' else 'F' end as src_order_type  
      ,order_date_id as Date,dim_item_id                      
      ,Avg(src_unit_cost) as src_unit_cost  
      ,Avg(src_current_retail_price) as src_current_retail_price  
      ,Avg(src_selling_price) as src_selling_price  
      ,0 as src_units_ordered  
      ,0 as src_units_shipped  
      ,sum(src_units_ordered) as Units_Returned  
      ,0 as "Demand_Cogs$"  
      ,0 as "Shipped_Cogs$"  
      ,sum(src_units_ordered*isnull(src_unit_cost,0)) as "Returned_Cogs$"  
      ,sum(src_units_ordered*isnull(src_unit_cost,0)) as "Net_Cogs$"  
      ,0 as "Demand_Retail$"  
      ,0 as "Shipped_Retail$"  
      ,sum ((src_units_ordered) *isnull(src_current_retail_price,0)) as "Net_Retail$"  
      ,0 as "Demand_Sales$"  
      ,0 as "Shipped_Sales$"  
      ,sum(-1* isnull(src_selling_price,0)) as "Return$"  
      ,sum(-1* isnull(src_selling_price,0)) as "Net_Sales_$"  
	From (select dim_business_unit_id,dim_shop_site_id,src_currency_code_from,  
            case src_order_type when 'C' then 'F' else 'F' end as src_order_type ,  
            dim_item_id,  
            src_order_number,  
            src_unit_cost,  
            src_selling_price,  
            src_current_retail_price,  
            src_units_ordered,  
            order_date_id    
   FROM {{source('fds_cpg','fact_cpg_sales_detail')}} 
		left join {{source('fds_cpg','dim_cpg_order_method')}} 
		on fact_cpg_sales_detail.dim_order_method_id = dim_cpg_order_method.dim_order_method_id    
	where (order_date_id>=0 or ship_date_id>=0) and   
			dim_item_id  not in (select distinct dim_item_id 
								from {{source('fds_cpg','dim_cpg_item')}}
								where src_item_id in ('990-000-002-0','990-000-003-0')) and  
			src_channel_id='R' and src_order_status='IN'  
			and dim_item_id not in (select distinct B.dim_item_id  
									from (select distinct B.dim_item_id as kit_id 
											from {{source('fds_cpg','dim_cpg_kit_item')}} A 
											inner join {{source('fds_cpg','dim_cpg_item')}} B  
											on A.src_kit_id=B.src_item_id) A, {{source('fds_cpg','dim_cpg_item')}} B 
									where A.kit_id=B.dim_item_id ) 
			and src_order_number  in (SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number  
										FROM {{source('fds_cpg','fact_cpg_sales_header')}} 
										where ltrim(rtrim(src_prepay_code))='F' and isnull(ltrim(rtrim(src_order_origin_code)),'AA')<>'GR')  
	) as A      
		left outer join  
        (select B.src_order_number as src_order_number,  
            A.dim_order_method_id as dim_order_method_id   
        from {{source('fds_cpg','fact_cpg_sales_header')}} A  
        inner join                         
        (select src_order_number,src_original_ref_order_number 
		from {{source('fds_cpg','fact_cpg_sales_header')}}  
        left join {{source('fds_cpg','dim_cpg_order_method')}} 
		on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dim_order_method_id 	
		where src_channel_id='R' ) B                                                              
        on A.src_order_number=B.src_order_number) B  
    on  A.src_order_number=B.src_order_number  
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,b.dim_order_method_id, src_order_type ,order_date_id,dim_item_id) Tab_Regular 
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id) Tab_Regular1  
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id  
--sub1 select end  
--Kit into Components---  
Union All  
--sub2 select  
Select dim_business_unit_id,dim_shop_site_id,src_currency_code_from    
      ,dim_order_method_id    
      ,case src_order_type when 'C' then 'F' else 'F' end as src_order_type      
      ,Date    
      ,dim_item_id    
      ,Avg(src_unit_cost) as src_unit_cost    
      ,Avg(src_current_retail_price) as src_current_retail_price    
      ,Sum(src_units_ordered) as src_units_ordered    
      ,sum(src_units_shipped) as src_units_shipped    
      ,sum(Units_Returned) as Units_Returned    
      ,sum(src_units_shipped+ Units_Returned) as Net_Units_Sold    
      ,sum("Demand_Cogs$") as "Demand_Cogs$"    
      ,sum ("Shipped_Cogs$") as "Shipped_Cogs$"    
      ,sum("Returned_Cogs$") as "Returned_Cogs$"    
      ,sum ("Net_Cogs$") as "Net_Cogs$"    
      ,sum("Demand_Retail$") as "Demand_Retail$"    
      ,sum ("Shipped_Retail$") as "Shipped_Retail$"    
      ,sum ("Net_Retail$") as "Net_Retail$"    
      ,sum("Demand_Sales$") as "Demand_Sales$"    
      ,sum("Shipped_Sales$") as "Shipped_Sales$"    
      ,sum("Return$") as "Return$"    
      ,sum("Net_Sales_$") as "Net_Sales_$"    
      ,sum("Demand_Selling_Margin$") as "Demand_Selling_Margin$"    
      ,sum("Shipped_Selling_Margin$") as "Shipped_Selling_Margin$"    
      ,sum("Net_Selling_Margin$") as "Net_Selling_Margin$"    
FROM    
/*Retrieving Demand sales for KIT Items*/    
(SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from    
      ,dim_order_method_id,src_order_type,Date,dim_item_id    
      ,Avg(src_unit_cost) as src_unit_cost    
      ,Avg(src_current_retail_price) as src_current_retail_price    
      ,Avg(src_selling_price) as src_selling_price    
      ,sum(src_units_ordered) as src_units_ordered    
      ,SUM(src_units_shipped) as src_units_shipped    
      ,SUM(Units_Returned) as Units_Returned    
      ,sum("Demand_Cogs$") as "Demand_Cogs$"    
      ,sum ("Shipped_Cogs$") as "Shipped_Cogs$"    
      ,sum("Returned_Cogs$") as "Returned_Cogs$"    
      ,sum ("Net_Cogs$") as "Net_Cogs$"    
      ,sum("Demand_Retail$") as "Demand_Retail$"    
      ,sum ("Shipped_Retail$") as "Shipped_Retail$"    
      ,sum ("Net_Retail$") as "Net_Retail$"    
      ,sum("Demand_Sales$") as "Demand_Sales$"    
      ,sum ("Shipped_Sales$") as "Shipped_Sales$"    
      ,sum("Return$") as "Return$"    
      ,sum("Net_Sales_$") as "Net_Sales_$"    
      ,sum(isnull("Demand_Sales$",0)-isnull("Demand_Cogs$",0)) as "Demand_Selling_Margin$"    
      ,sum(isnull("Shipped_Sales$",0)-isnull("Shipped_Cogs$",0)) as "Shipped_Selling_Margin$"    
      ,sum(isnull("Net_Sales_$",0)-isnull("Net_Cogs$",0)) as "Net_Selling_Margin$"    
	from (SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from    
      ,fact_cpg_sales_detail_kit_component.dim_order_method_id    
      ,src_order_type,order_date_id as Date,dim_item_id    
      ,Avg(src_unit_cost) as src_unit_cost    
      ,Avg(src_current_retail_price) as src_current_retail_price    
      ,Avg(((src_kit_selling_price*src_component_percent)/100)/(src_required_quantity)) as src_selling_price   
      ,sum(src_kit_units_ordered ) as src_units_ordered    
      ,0 as src_units_shipped    
      ,0 as Units_Returned    
      ,sum(src_required_quantity) as src_required_quantity    
      ,sum(src_kit_units_ordered  *isnull(src_unit_cost,0)) as "Demand_Cogs$"    
      ,0 as "Shipped_Cogs$"    
      ,0 as "Returned_Cogs$"    
      ,0 as "Net_Cogs$"    
      ,sum(src_kit_units_ordered  * isnull(src_current_retail_price,0)) as "Demand_Retail$"    
      ,0 as "Shipped_Retail$"    
      ,0 as "Net_Retail$"    
	  ,sum(src_kit_units_ordered * ((isnull(src_kit_selling_price,0)*src_component_percent)/100)) as "Demand_Sales$"    
      ,0 as "Shipped_Sales$"    
      ,0 as "Return$"    
      ,0 as "Net_Sales_$"    
    FROM {{source('fds_cpg','fact_cpg_sales_detail_kit_component')}}    
        left join {{source('fds_cpg','dim_cpg_order_method')}} 
		on fact_cpg_sales_detail_kit_component.dim_order_method_id= dim_cpg_order_method.dim_order_method_id
    where (order_date_id>=0 or ship_date_id>=0) and     
            src_order_type='I'  and isnull(src_channel_id,'0')<>'R' and    
            src_order_number  in (SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number  
									FROM {{source('fds_cpg','fact_cpg_sales_header')}}
									where ltrim(rtrim(src_prepay_code))='F' and isnull(ltrim(rtrim(src_order_origin_code)),'AA')<>'GR')      
            and src_order_number  in (SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number    
										FROM {{source('fds_cpg','fact_cpg_sales_header')}}    
										where ltrim(rtrim(src_original_ref_order_number))='0')     
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,fact_cpg_sales_detail_kit_component.dim_order_method_id,src_order_type,order_date_id,dim_item_id,src_kit_selling_price) As A    
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id,src_order_type,Date,dim_item_id                   
    UNION all    
/*Retrieving Shipped sales for Kit Items*/        
SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from    
      ,dim_order_method_id    
      ,src_order_type,Date,dim_item_id    
      ,Avg(src_unit_cost) as src_unit_cost    
      ,Avg(src_current_retail_price) as src_current_retail_price    
      ,Avg(src_selling_price) as src_selling_price    
      ,sum(src_units_ordered) as src_units_ordered    
      ,SUM(src_units_shipped) as src_units_shipped    
      ,SUM(Units_Returned) as Units_Returned    
      ,sum("Demand_Cogs$") as "Demand_Cogs$"    
      ,sum ("Shipped_Cogs$") as "Shipped_Cogs$"    
      ,sum("Returned_Cogs$") as "Returned_Cogs$"    
	  ,sum ("Net_Cogs$") as "Net_Cogs$"    
      ,sum("Demand_Retail$") as "Demand_Retail$"    
      ,sum ("Shipped_Retail$") as "Shipped_Retail$"    
      ,sum ("Net_Retail$") as "Net_Retail$"    
      ,sum("Demand_Sales$") as "Demand_Sales$"    
      ,sum("Shipped_Sales$") as "Shipped_Sales$"    
      ,sum("Return$") as "Return$"    
      ,sum("Net_Sales_$") as "Net_Sales_$"    
      ,sum(isnull("Demand_Sales$",0)-isnull("Demand_Cogs$",0)) as "Demand_Selling_Margin$"    
      ,sum(isnull("Shipped_Sales$",0)-isnull("Shipped_Cogs$",0)) as "Shipped_Selling_Margin$"    
      ,sum(isnull("Net_Sales_$",0)-isnull("Net_Cogs$",0)) as "Net_Selling_Margin$"    
    from (SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from    
      ,fact_cpg_sales_detail_kit_component.dim_order_method_id    
      ,src_order_type,ship_date_id as Date,dim_item_id    
      ,Avg(src_unit_cost) as src_unit_cost    
      ,Avg(src_current_retail_price) as src_current_retail_price    
      ,Avg(((src_kit_selling_price*src_component_percent)/100)/(src_required_quantity)) as src_selling_price    
      ,0 as src_units_ordered    
      ,sum(src_kit_units_shipped ) as src_units_shipped    
      ,0 as Units_Returned    
      ,sum(src_required_quantity) as src_required_quantity    
      ,0 as "Demand_Cogs$"    
      ,sum (src_kit_units_shipped *isnull(src_unit_cost,0)) as "Shipped_Cogs$"    
      ,0 as "Returned_Cogs$"    
      ,sum (src_kit_units_shipped *isnull(src_unit_cost,0)) as "Net_Cogs$"    
      ,0 as "Demand_Retail$"    
      ,sum (src_kit_units_shipped * isnull(src_current_retail_price,0)) as "Shipped_Retail$"    
      ,sum ((src_kit_units_shipped)  *isnull(src_current_retail_price,0)) as "Net_Retail$"    
      ,0 as "Demand_Sales$"    
      ,sum(src_kit_units_shipped*((isnull(src_kit_selling_price,0)*src_component_percent)/100)) as "Shipped_Sales$"    
      ,0 as "Return$"    
      ,sum(src_kit_units_shipped*((isnull(src_kit_selling_price,0)*src_component_percent)/100)) as "Net_Sales_$"    
		FROM {{source('fds_cpg','fact_cpg_sales_detail_kit_component')}}    
            left join {{source('fds_cpg','dim_cpg_order_method')}} 
			on fact_cpg_sales_detail_kit_component.dim_order_method_id= dim_cpg_order_method.dim_order_method_id				
        where (order_date_id>=0 or ship_date_id>=0) and     
                dim_item_id  not in (select distinct dim_item_id 
									from {{source('fds_cpg','dim_cpg_item')}} 
									where src_item_id in ('990-000-002-0','990-000-003-0')) and    
                src_order_type='I' and ship_date_id is not null and isnull(src_channel_id,'0')<>'R' 
                and src_order_number  in (SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number  
											FROM {{source('fds_cpg','fact_cpg_sales_header')}} 
											where ltrim(rtrim(src_prepay_code))='F' and isnull(ltrim(rtrim(src_order_origin_code)),'AA')<>'GR')  
                and src_order_status='IN'     
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,fact_cpg_sales_detail_kit_component.dim_order_method_id,src_order_type,ship_date_id,dim_item_id,src_kit_selling_price) As A    
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id,src_order_type,Date,dim_item_id    
    UNION ALL    
/*Retrieving Return sales for Kit Items*/          
SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from    
      ,dim_order_method_id    
      ,case src_order_type when 'C' then 'F' else 'F' end as src_order_type           
      ,Date,dim_item_id    
      ,Avg(src_unit_cost) as src_unit_cost    
      ,Avg(src_current_retail_price) as src_current_retail_price    
      ,Avg(src_selling_price) as src_selling_price    
      ,sum(src_units_ordered) as src_units_ordered    
      ,SUM(src_units_shipped) as src_units_shipped    
      ,SUM(Units_Returned) as Units_Returned    
      ,sum("Demand_Cogs$") as "Demand_Cogs$"    
      ,sum ("Shipped_Cogs$") as "Shipped_Cogs$"    
      ,sum("Returned_Cogs$") as "Returned_Cogs$"    
      ,sum ("Net_Cogs$") as "Net_Cogs$"    
      ,sum("Demand_Retail$") as "Demand_Retail$"    
      ,sum ("Shipped_Retail$") as "Shipped_Retail$"    
      ,sum ("Net_Retail$") as "Net_Retail$"    
      ,sum("Demand_Sales$") as "Demand_Sales$"    
      ,sum ("Shipped_Sales$") as "Shipped_Sales$"    
      ,sum("Return$") as "Return$"    
      ,sum("Net_Sales_$") as "Net_Sales_$"    
      ,sum(isnull("Demand_Sales$",0)-isnull("Demand_Cogs$",0)) as "Demand_Selling_Margin$"    
      ,sum(isnull("Shipped_Sales$",0)-isnull("Shipped_Cogs$",0)) as "Shipped_Selling_Margin$"    
      ,sum(isnull("Net_Sales_$",0)-isnull("Net_Cogs$",0)) as "Net_Selling_Margin$"    
from (SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from    
      ,fact_cpg_sales_detail_kit_component.dim_order_method_id    
      ,case src_order_type when 'C' then 'F' else 'F' end as src_order_type         
      ,order_date_id as Date,dim_item_id    
      ,avg(src_unit_cost) as src_unit_cost    
      ,Avg(src_current_retail_price) as src_current_retail_price    
      ,Avg(((src_kit_selling_price*src_component_percent)/100)/(src_required_quantity)) as src_selling_price    
      ,0 as src_units_ordered    
      ,0 as src_units_shipped    
      ,sum(src_kit_units_ordered ) as Units_Returned    
      ,sum(src_required_quantity) as src_required_quantity    
      ,0 as "Demand_Cogs$"    
      ,0 as "Shipped_Cogs$"    
      ,sum(src_kit_units_ordered *isnull(src_unit_cost,0)) as "Returned_Cogs$"    
      ,sum(src_kit_units_ordered *isnull(src_unit_cost,0)) as "Net_Cogs$"    
      ,0 as "Demand_Retail$"    
      ,0 as "Shipped_Retail$"    
      ,sum ((src_kit_units_ordered)  *isnull(src_current_retail_price,0)) as "Net_Retail$"    
      ,0 as "Demand_Sales$"    
      ,0 as "Shipped_Sales$"    
      ,sum((src_kit_units_ordered * isnull(src_kit_selling_price,0)*src_component_percent)/100) as "Return$"    
      ,sum(src_kit_units_ordered* ((isnull(src_kit_selling_price,0)*src_component_percent)/100)) as "Net_Sales_$"    
        FROM {{source('fds_cpg','fact_cpg_sales_detail_kit_component')}} 
			left join {{source('fds_cpg','dim_cpg_order_method')}} 
			on fact_cpg_sales_detail_kit_component.dim_order_method_id= dim_cpg_order_method.dim_order_method_id 
		where (order_date_id>=0 or ship_date_id>=0) and     
				dim_item_id  not in (select distinct dim_item_id 
									from {{source('fds_cpg','dim_cpg_item')}} 
									where src_item_id in ('990-000-002-0','990-000-003-0')) and    
				src_channel_id='R' and src_order_status='IN' 
				and src_order_number  in (SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number  
											FROM {{source('fds_cpg','fact_cpg_sales_header')}} 
											where ltrim(rtrim(src_prepay_code))='F' and isnull(ltrim(rtrim(src_order_origin_code)),'AA')<>'GR')  
	group by  dim_business_unit_id,dim_shop_site_id,src_currency_code_from, fact_cpg_sales_detail_kit_component.dim_order_method_id,  
		src_order_type,dim_item_id,src_order_number,src_unit_cost,src_kit_selling_price,src_current_retail_price,src_kit_units_ordered,    
        order_date_id) Tab_Kit_Items    
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id) Tab_Kit_Regular_Order  
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id) Sub_Main    
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id