

with #fact_aggregate_sales_temp1 as
(select * from(
select   
dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
        ,dim_order_method_id  
        ,src_order_type  
        ,Date as Date_Key  
        ,dim_item_id  
      ,0 as Other_Amount
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
        ,sum("Net_sales_$") as "Net_sales_$"  
        ,sum("Demand_Selling_Margin$") as "Demand_Selling_Margin$"  
        ,sum("Shipped_Selling_Margin$") as "Shipped_Selling_Margin$"  
        ,sum("Net_Selling_Margin$") as "Net_Selling_Margin$"  
        --  ,current_date as create_timestamp
        --,'ETL' as created_by
        --,null as update_timestamp
        --,null as updated_by
        --,null as venue_key
        --,null as venue_flag 
            from   
    (       
  --sub1 select  
  Select dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
        ,dim_order_method_id  
        ,case src_order_type when 'C' then 'I' else 'I' end as src_order_type   
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
        , sum("Demand_Sales$") as "Demand_Sales$"  
        ,sum("Shipped_Sales$") as "Shipped_Sales$"  
        , sum("Return$") as "Return$"  
        , sum("Net_sales_$") as "Net_sales_$"  
        , sum("Demand_Selling_Margin$") as "Demand_Selling_Margin$"  
        , sum("Shipped_Selling_Margin$") as "Shipped_Selling_Margin$"  
         ,sum("Net_Selling_Margin$") as "Net_Selling_Margin$"  
    FROM  
  (  
  /*Retrieving Demand sales for Regular Items*/  
              SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
                     ,dim_order_method_id  
                                       ,src_order_type         
               ,Date  
                                       ,dim_item_id  
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
        ,sum("Net_sales_$") as "Net_sales_$"  
        ,sum(isnull("Demand_Sales$",0)-isnull("Demand_Cogs$",0)) as "Demand_Selling_Margin$"  
        ,sum(isnull("Shipped_Sales$",0)-isnull("Shipped_Cogs$",0)) as "Shipped_Selling_Margin$"  
         ,sum(isnull("Net_sales_$",0)-isnull("Net_Cogs$",0)) as "Net_Selling_Margin$"  
               from (SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
                                       ,fact_cpg_sales_detail.dim_order_method_id  
                                       ,src_order_type         
               ,order_date_id as Date  
                                       ,dim_item_id  
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
   FROM "entdwdb"."fds_cpg"."fact_cpg_sales_detail"(nolock)
   left join
   "entdwdb"."fds_cpg"."dim_cpg_order_method" on fact_cpg_sales_detail.dim_order_method_id= dim_cpg_order_method.dim_order_method_id   where   
                  (order_date_id>19000101 or ship_date_id>19000101) and   
                  dim_item_id  not in (select distinct dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id in ('990-000-002-0','990-000-003-0')) and  
                  src_order_type='I' and isnull(src_channel_id,'0') NOT IN ('R','VS') and  
                   dim_item_id not in 
				   (select distinct B.dim_item_id  from (select distinct B.dim_item_id as kit_id from "entdwdb"."fds_cpg"."dim_cpg_kit_item"(nolock) A 
inner join "entdwdb"."fds_cpg"."dim_cpg_item" (nolock) B  on A.src_kit_id=B.src_item_id
) A, "entdwdb"."fds_cpg"."dim_cpg_item"(NoLock) B 
where A.kit_id=B.dim_item_id )
                  and src_order_number not in (SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number  
                        FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock) where ltrim(rtrim(src_order_origin_code))='GR' or ltrim(rtrim(src_prepay_code))='F')  
                        and src_order_number  in (SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number  
                        FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock) where ltrim(rtrim(src_original_ref_order_number))='0')   
                        group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,fact_cpg_sales_detail.dim_order_method_id, src_order_type ,order_date_id,dim_item_id) Tab1  
                        group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id               
              UNION all  
/*Retrieving Shipped sales for Regular Items*/           
              SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
                                     ,dim_order_method_id  
                                     ,src_order_type         
             ,Date  
                                     ,dim_item_id  
             ,Avg(src_unit_cost) as src_unit_cost  
                                     ,Avg(src_current_retail_price) as src_current_retail_price  
                                     ,Avg(src_selling_price) as src_selling_price  
                                     ,sum(src_units_ordered) as src_units_ordered  
                                     , SUM(src_units_shipped) as src_units_shipped  
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
            from (  
              SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
                                     ,fact_cpg_sales_detail.dim_order_method_id  
                                     ,src_order_type  
                                     ,ship_date_id as Date  
                                     ,dim_item_id                      
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
            FROM "entdwdb"."fds_cpg"."fact_cpg_sales_detail"(nolock)
 left join
   "entdwdb"."fds_cpg"."dim_cpg_order_method" on fact_cpg_sales_detail.dim_order_method_id= dim_cpg_order_method.dim_order_method_id 			
      where  
(order_date_id>19000101 or ship_date_id>19000101) and   
 dim_item_id  not in (select distinct dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id in ('990-000-002-0','990-000-003-0')) and  
      src_order_type='I' and ship_date_id is not null and isnull(src_channel_id,'0') NOT IN ('R','VS') and  
                        dim_item_id not in 
						(select distinct B.dim_item_id  from (select distinct B.dim_item_id as kit_id from "entdwdb"."fds_cpg"."dim_cpg_kit_item"(nolock) A 
inner join "entdwdb"."fds_cpg"."dim_cpg_item" (nolock) B  on A.src_kit_id=B.src_item_id
) A, "entdwdb"."fds_cpg"."dim_cpg_item"(NoLock) B 
where A.kit_id=B.dim_item_id )
                        and src_order_number not in (SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number  
                        FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock) where ltrim(rtrim(src_order_origin_code))='GR' or ltrim(rtrim(src_prepay_code))='F')  
                        and src_order_status='IN'  
                        group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,fact_cpg_sales_detail.dim_order_method_id, src_order_type ,ship_date_id,dim_item_id) Tab2  
                        group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id  
            UNION ALL  
/*Retrieving Return sales for Regular Items*/             
SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
                                     ,dim_order_method_id  
                                    ,case src_order_type when 'C' then 'I' else 'I' end as src_order_type        
             , Date  
                                     ,dim_item_id  
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
            from (  
SELECT                        dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
                                    ,b.dim_order_method_id as dim_order_method_id  
                                    ,case src_order_type when 'C' then 'I' else 'I' end as src_order_type  
                                    ,order_date_id as Date  
                                    ,dim_item_id                      
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
From  
            (select dim_business_unit_id,dim_shop_site_id,src_currency_code_from,  
            case src_order_type when 'C' then 'I' else 'I' end as src_order_type ,  
            dim_item_id,  
            src_order_number,  
            src_unit_cost,  
            src_selling_price,  
            src_current_retail_price,  
            src_units_ordered,  
            order_date_id    
   FROM "entdwdb"."fds_cpg"."fact_cpg_sales_detail"(nolock)
 left join
   "entdwdb"."fds_cpg"."dim_cpg_order_method" on fact_cpg_sales_detail.dim_order_method_id= dim_cpg_order_method.dim_order_method_id    where   
   (order_date_id>19000101 or ship_date_id>19000101) and   
   dim_item_id  not in (select distinct dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id in ('990-000-002-0','990-000-003-0')) and  
    src_channel_id='R' and src_order_status='IN'  
   and dim_item_id not in 
					(select distinct B.dim_item_id  from (select distinct B.dim_item_id as kit_id from "entdwdb"."fds_cpg"."dim_cpg_kit_item"(nolock) A 
inner join "entdwdb"."fds_cpg"."dim_cpg_item" (nolock) B  on A.src_kit_id=B.src_item_id
) A, "entdwdb"."fds_cpg"."dim_cpg_item"(NoLock) B 
where A.kit_id=B.dim_item_id )
    and src_order_number not in (SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number  
    FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock) where ltrim(rtrim(src_order_origin_code))='GR' or ltrim(rtrim(src_prepay_code))='F')  
   ) as A       
left outer join  
            (select B.src_order_number as src_order_number,  
            A.dim_order_method_id as dim_order_method_id   
            from "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock) A  
            inner join                         
                                    (select src_order_number,src_original_ref_order_number from "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock)
 left join
   "entdwdb"."fds_cpg"."dim_cpg_order_method" on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dim_order_method_id 									
                                                            where src_channel_id='R' ) B                                                              
                                                            on A.src_order_number=B.src_order_number   ) B  
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
      ,case src_order_type when 'C' then 'I' else 'I' end as src_order_type    
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
      , sum("Demand_Sales$") as "Demand_Sales$"  
      ,sum("Shipped_Sales$") as "Shipped_Sales$"  
      , sum("Return$") as "Return$"  
      , sum("Net_Sales_$") as "Net_Sales_$"  
      , sum("Demand_Selling_Margin$") as "Demand_Selling_Margin$"  
      , sum("Shipped_Selling_Margin$") as "Shipped_Selling_Margin$"  
      ,sum("Net_Selling_Margin$") as "Net_Selling_Margin$"  
FROM  
/*Retrieving Demand sales for KIT Items*/  
(SELECT	dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
									,dim_order_method_id  
                                     ,src_order_type       
             ,Date  
                                     ,dim_item_id  
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
            from (   
            SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
                                     ,fact_cpg_sales_detail_kit_component.dim_order_method_id  
                                     ,src_order_type         
             ,order_date_id as Date  
                                     ,dim_item_id  
             ,Avg(src_unit_cost) as src_unit_cost  
                                     ,Avg(src_current_retail_price) as src_current_retail_price  
                                     ,Avg(((src_kit_selling_price*src_component_percent)/100)/(nullif(src_required_quantity,0))) as src_selling_price  --Has to Remove  
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
                FROM   
               "entdwdb"."fds_cpg"."fact_cpg_sales_detail_kit_component"(nolock)
			   left join
   "entdwdb"."fds_cpg"."dim_cpg_order_method" on fact_cpg_sales_detail_kit_component.dim_order_method_id= dim_cpg_order_method.dim_order_method_id
                                    where   
                                    (order_date_id>19000101 or ship_date_id>19000101) and   
                                    src_order_type='I'  and isnull(src_channel_id,'0')<>'R' and  
                                  src_order_number not in   
          (  
          SELECT  distinct ltrim(rtrim(src_order_number)) As src_order_number  
           FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock)
            where   
            ltrim(rtrim(src_order_origin_code))='GR' or   
            ltrim(rtrim(src_prepay_code))='F')  
            and src_order_number  in (  
            SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number  
            FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock)
            where   
            ltrim(rtrim(src_original_ref_order_number))='0')   
         group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,fact_cpg_sales_detail_kit_component.dim_order_method_id, src_order_type ,order_date_id,dim_item_id,src_kit_selling_price) As A  
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id,src_order_type,Date,dim_item_id                 
              UNION all  
/*Retrieving Shipped sales for Kit Items*/      
             SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
                                     ,dim_order_method_id  
                                     ,src_order_type        
             ,Date  
                                     ,dim_item_id  
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
            from (  
 SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
                                     ,fact_cpg_sales_detail_kit_component.dim_order_method_id  
                                     ,src_order_type         
             ,ship_date_id as Date  
                                     ,dim_item_id  
             ,Avg(src_unit_cost) as src_unit_cost  
                                     ,Avg(src_current_retail_price) as src_current_retail_price  
                                     ,Avg(((src_kit_selling_price*src_component_percent)/100)/(nullif(src_required_quantity,0))) as src_selling_price  
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
                FROM   
                "entdwdb"."fds_cpg"."fact_cpg_sales_detail_kit_component"(nolock)
 left join
   "entdwdb"."fds_cpg"."dim_cpg_order_method" on fact_cpg_sales_detail_kit_component.dim_order_method_id= dim_cpg_order_method.dim_order_method_id				
                                    where   
                                    (order_date_id>19000101 or ship_date_id>19000101) and   
                                    dim_item_id  not in (select distinct dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id in ('990-000-002-0','990-000-003-0')) and  
                                    src_order_type='I' and ship_date_id is not null and isnull(src_channel_id,'0')<>'R'   
                                    and src_order_number not in (SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number  
                                    FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock) where ltrim(rtrim(src_order_origin_code))='GR' or ltrim(rtrim(src_prepay_code))='F')  
                                    and src_order_status='IN'   
                                    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,fact_cpg_sales_detail_kit_component.dim_order_method_id, src_order_type ,ship_date_id,dim_item_id,src_kit_selling_price) As A  
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
      ,dim_order_method_id,src_order_type,Date,dim_item_id  
            UNION ALL  
/*Retrieving Return sales for Kit Items*/        
              SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
                                     ,dim_order_method_id  
                                     ,case src_order_type when 'C' then 'I' else 'I' end as src_order_type         
             ,Date  
                                     ,dim_item_id  
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
            from (  
SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
                                     ,fact_cpg_sales_detail_kit_component.dim_order_method_id  
                                    ,case src_order_type when 'C' then 'I' else 'I' end as src_order_type       
             ,order_date_id as Date  
                                     ,dim_item_id  
             ,Avg(src_unit_cost) as src_unit_cost  
                                     ,Avg(src_current_retail_price) as src_current_retail_price  
                                     ,Avg(((src_kit_selling_price*src_component_percent)/100)/(nullif(src_required_quantity,0))) as src_selling_price  
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
                FROM   
"entdwdb"."fds_cpg"."fact_cpg_sales_detail_kit_component"(nolock) 
 left join
   "entdwdb"."fds_cpg"."dim_cpg_order_method" on fact_cpg_sales_detail_kit_component.dim_order_method_id= dim_cpg_order_method.dim_order_method_id where    
   (order_date_id>19000101 or ship_date_id>19000101) and   
   dim_item_id  not in (select distinct dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id in ('990-000-002-0','990-000-003-0')) and  
    src_channel_id='R' and src_order_status='IN'  
   and src_order_number not in (SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number  
   FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock) where ltrim(rtrim(src_order_origin_code))='GR' or ltrim(rtrim(src_prepay_code))='F')  
   group by  dim_business_unit_id,dim_shop_site_id,src_currency_code_from, fact_cpg_sales_detail_kit_component.dim_order_method_id,
            src_order_type,  
            dim_item_id,  
            src_order_number,  
            src_unit_cost,  
            src_kit_selling_price,  
            src_current_retail_price,  
            src_kit_units_ordered,  
            order_date_id   
   ) Tab_Kit_Items  
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id) Tab_Kit_Regular_Orders  
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id) Sub_Main  
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id)),
--[SP_Aggregate_Gratis_Orders]
#fact_aggregate_sales_temp2 as
(select * from(
select dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
      ,dim_order_method_id  
      ,src_order_type  
      ,Date as Date_Key  
      ,dim_item_id  
      ,0 as Other_Amount
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
       --  ,current_date as create_timestamp
       -- ,'ETL' as created_by
       -- ,null as update_timestamp
       -- ,null as updated_by
       -- ,null as venue_key
       -- ,null as venue_flag
      from   
  (       
--sub1 select  
Select dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
      ,dim_order_method_id  
      ,case src_order_type when 'C' then 'GR' else 'GR' end as src_order_type   
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
      , sum("Demand_Sales$") as "Demand_Sales$"  
      ,sum("Shipped_Sales$") as "Shipped_Sales$"  
      , sum("Return$") as "Return$"  
      , sum("Net_Sales_$") as "Net_Sales_$"  
      , sum("Demand_Selling_Margin$") as "Demand_Selling_Margin$"  
      , sum("Shipped_Selling_Margin$") as "Shipped_Selling_Margin$"  
       ,sum("Net_Selling_Margin$") as "Net_Selling_Margin$"  
FROM  
(  
/*Retrieving Demand sales for Gratis Items*/  
            SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
                                     ,dim_order_method_id  
                                     ,case src_order_type when 'I' then 'GR' else 'GR' end as src_order_type         
             ,Date  
                                     ,dim_item_id  
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
                                     ,src_order_type         
             ,order_date_id as Date  
                                     ,dim_item_id  
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
                FROM "entdwdb"."fds_cpg"."fact_cpg_sales_detail" (nolock)
left join
   "entdwdb"."fds_cpg"."dim_cpg_order_method" on fact_cpg_sales_detail.dim_order_method_id= dim_cpg_order_method.dim_order_method_id   where   
   
                        (order_date_id>19000101 or ship_date_id>19000101) and   
                        dim_item_id  not in (select distinct dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id in ('990-000-002-0','990-000-003-0')) and  
                        src_order_type='I' and isnull(src_channel_id,'0')<>'R' and  
                        dim_item_id not in
						
 (select distinct B.dim_item_id  from (select distinct B.dim_item_id as kit_id from "entdwdb"."fds_cpg"."dim_cpg_kit_item"(nolock) A 
inner join "entdwdb"."fds_cpg"."dim_cpg_item" (nolock) B  on A.src_kit_id=B.src_item_id
) A, "entdwdb"."fds_cpg"."dim_cpg_item"(NoLock) B 
where A.kit_id=B.dim_item_id )
                        and src_order_number  in (SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number  
                        FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock) where ltrim(rtrim(src_order_origin_code))='GR')  
                        group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,fact_cpg_sales_detail.dim_order_method_id, src_order_type ,order_date_id,dim_item_id) Tab1  
                        group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id             
              UNION all  
/*Retrieving Shipped sales for Gratis Items*/           
              SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
                                     ,dim_order_method_id  
                                     ,case src_order_type when 'I' then 'GR' else 'GR' end as src_order_type         
             ,Date  
                                     ,dim_item_id  
             ,Avg(src_unit_cost) as src_unit_cost  
                                     ,Avg(src_current_retail_price) as src_current_retail_price  
                                     ,Avg(src_selling_price) as src_selling_price  
                                     ,sum(src_units_ordered) as src_units_ordered  
                                     , SUM(src_units_shipped) as src_units_shipped  
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
            from (  
              SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
                                       ,fact_cpg_sales_detail.dim_order_method_id  
                                     ,src_order_type  
                                     ,ship_date_id as Date  
                                     ,dim_item_id                      
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
      FROM "entdwdb"."fds_cpg"."fact_cpg_sales_detail"(nolock)  
      left join
   "entdwdb"."fds_cpg"."dim_cpg_order_method" on fact_cpg_sales_detail.dim_order_method_id= dim_cpg_order_method.dim_order_method_id 			
      where  
      (order_date_id>19000101 or ship_date_id>19000101) and   
       dim_item_id  not in (select distinct dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id in ('990-000-002-0','990-000-003-0')) and  
       src_order_type='I' and ship_date_id is not null and isnull(src_channel_id,'0')<>'R' and  
       dim_item_id not in
 (select distinct B.dim_item_id  from (select distinct B.dim_item_id as kit_id from "entdwdb"."fds_cpg"."dim_cpg_kit_item"(nolock) A 
inner join "entdwdb"."fds_cpg"."dim_cpg_item" (nolock) B  on A.src_kit_id=B.src_item_id
) A, "entdwdb"."fds_cpg"."dim_cpg_item"(NoLock) B 
where A.kit_id=B.dim_item_id )
                        and src_order_number  in (SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number  
                        FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock) where ltrim(rtrim(src_order_origin_code))='GR')  
                        and src_order_status='IN'  
                        group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,fact_cpg_sales_detail.dim_order_method_id, src_order_type ,ship_date_id,dim_item_id) Tab2  
                        group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id  
            UNION ALL  
/*Retrieving Return sales for Gratis Items*/             
SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
                                     ,dim_order_method_id  
                                    ,case src_order_type when 'C' then 'GR' else 'GR' end as src_order_type        
             , Date  
                                     ,dim_item_id  
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
            from (  
SELECT                        dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
           ,b.dim_order_method_id as dim_order_method_id  
                                    ,case src_order_type when 'C' then 'GR' else 'GR' end as src_order_type  
                                    ,order_date_id as Date  
                                    ,dim_item_id                      
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
From  
            (select dim_business_unit_id,dim_shop_site_id,src_currency_code_from,  
            case src_order_type when 'C' then 'GR' else 'GR' end as src_order_type ,  
            dim_item_id,  
            src_order_number,  
            src_unit_cost,  
            src_selling_price,  
            src_current_retail_price,  
            src_units_ordered,  
            order_date_id    
   FROM "entdwdb"."fds_cpg"."fact_cpg_sales_detail" (nolock) left join
   "entdwdb"."fds_cpg"."dim_cpg_order_method" on fact_cpg_sales_detail.dim_order_method_id= dim_cpg_order_method.dim_order_method_id       
   where   
   (order_date_id>19000101 or ship_date_id>19000101) and   
    dim_item_id  not in (select distinct dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id in ('990-000-002-0','990-000-003-0')) and  
   src_channel_id='R' and src_order_status='IN'  
   and  
            dim_item_id not in
(select distinct B.dim_item_id  from (select distinct B.dim_item_id as kit_id from "entdwdb"."fds_cpg"."dim_cpg_kit_item"(nolock) A 
inner join "entdwdb"."fds_cpg"."dim_cpg_item" (nolock) B  on A.src_kit_id=B.src_item_id
) A, "entdwdb"."fds_cpg"."dim_cpg_item"(NoLock) B 
where A.kit_id=B.dim_item_id ) 
            and src_order_number  in (SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number  
                        FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock) where ltrim(rtrim(src_order_origin_code))='GR')  
   ) as A      
left outer join  
            (select B.src_order_number as src_order_number,  
            A.dim_order_method_id as dim_order_method_id   
            from "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock) A  
            inner join                         
                                    (select src_order_number,src_original_ref_order_number from "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock)  
left join
   "entdwdb"."fds_cpg"."dim_cpg_order_method" on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dim_order_method_id 	
                                                            where src_channel_id='R' ) B                                                              
                                                            on A.src_order_number=B.src_order_number   ) B  
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
      ,case src_order_type when 'C' then 'GR' else 'GR' end as src_order_type      
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
      , sum("Demand_Sales$") as "Demand_Sales$"    
      ,sum("Shipped_Sales$") as "Shipped_Sales$"    
      , sum("Return$") as "Return$"    
      , sum("Net_Sales_$") as "Net_Sales_$"    
      , sum("Demand_Selling_Margin$") as "Demand_Selling_Margin$"    
      , sum("Shipped_Selling_Margin$") as "Shipped_Selling_Margin$"    
      ,sum("Net_Selling_Margin$") as "Net_Selling_Margin$"    
FROM    
/*Retrieving Demand sales for KIT Items*/    
(SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from    
         ,dim_order_method_id    
                                     ,src_order_type         
             ,Date    
                                     ,dim_item_id    
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
            from (     
            SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from    
                                     ,fact_cpg_sales_detail_kit_component.dim_order_method_id    
                                     ,src_order_type           
             ,order_date_id as Date    
                                     ,dim_item_id    
             ,Avg(src_unit_cost) as src_unit_cost    
                                     ,Avg(src_current_retail_price) as src_current_retail_price    
                                     ,Avg(((src_kit_selling_price*src_component_percent)/100)/(nullif(src_required_quantity,0))) as src_selling_price  --Has to Remove    
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
                FROM     
                "entdwdb"."fds_cpg"."fact_cpg_sales_detail_kit_component"(nolock)    
                                  left join
   "entdwdb"."fds_cpg"."dim_cpg_order_method" on fact_cpg_sales_detail_kit_component.dim_order_method_id= dim_cpg_order_method.dim_order_method_id
                                     where     
                                    (order_date_id>19000101 or ship_date_id>19000101) and     
                                    src_order_type='I'  and isnull(src_channel_id,'0')<>'R' and    
                                   src_order_number  in (SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number  
                        FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock) where ltrim(rtrim(src_order_origin_code))='GR')     
            and src_order_number  in (    
            SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number    
            FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock)    
            where     
            ltrim(rtrim(src_original_ref_order_number))='0')     
         group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,fact_cpg_sales_detail_kit_component.dim_order_method_id, src_order_type ,order_date_id,dim_item_id,src_kit_selling_price) As A    
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id,src_order_type,Date,dim_item_id                   
              UNION all    
/*Retrieving Shipped sales for Kit Items*/        
             SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from    
                                     ,dim_order_method_id    
                                     ,src_order_type          
             ,Date    
                                     ,dim_item_id    
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
            from (    
 SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from    
                                     ,fact_cpg_sales_detail_kit_component.dim_order_method_id    
                                     ,src_order_type           
             ,ship_date_id as Date    
                                     ,dim_item_id    
             ,Avg(src_unit_cost) as src_unit_cost    
                                     ,Avg(src_current_retail_price) as src_current_retail_price    
                                     ,Avg(((src_kit_selling_price*src_component_percent)/100)/(nullif(src_required_quantity,0))) as src_selling_price    
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
                FROM     
                "entdwdb"."fds_cpg"."fact_cpg_sales_detail_kit_component"(nolock)
left join
   "entdwdb"."fds_cpg"."dim_cpg_order_method" on fact_cpg_sales_detail_kit_component.dim_order_method_id= dim_cpg_order_method.dim_order_method_id				
    where     
                                    (order_date_id>19000101 or ship_date_id>19000101) and     
                                    dim_item_id  not in (select distinct dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id in ('990-000-002-0','990-000-003-0')) and    
                                    src_order_type='I' and ship_date_id is not null and isnull(src_channel_id,'0')<>'R' 
                                   and src_order_number  in (SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number  
                        FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock) where ltrim(rtrim(src_order_origin_code))='GR') 
                                    and src_order_status='IN'     
                                    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,fact_cpg_sales_detail_kit_component.dim_order_method_id, src_order_type ,ship_date_id,dim_item_id,src_kit_selling_price) As A    
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from    
      ,dim_order_method_id,src_order_type,Date,dim_item_id    
            UNION ALL    
/*Retrieving Return sales for Kit Items*/          
              SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from    
                                     ,dim_order_method_id    
                                     ,case src_order_type when 'C' then 'GR' else 'GR' end as src_order_type           
             ,Date    
                                     ,dim_item_id    
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
            from (    
SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from    
                                     ,fact_cpg_sales_detail_kit_component.dim_order_method_id    
                                    ,case src_order_type when 'C' then 'GR' else 'GR' end as src_order_type         
             ,order_date_id as Date    
                                     ,dim_item_id    
             ,Avg(src_unit_cost) as src_unit_cost    
                                     ,Avg(src_current_retail_price) as src_current_retail_price    
                                     ,Avg(((src_kit_selling_price*src_component_percent)/100)/(nullif(src_required_quantity,0))) as src_selling_price    
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
                FROM     
"entdwdb"."fds_cpg"."fact_cpg_sales_detail_kit_component"(nolock)  left join
   "entdwdb"."fds_cpg"."dim_cpg_order_method" on fact_cpg_sales_detail_kit_component.dim_order_method_id= dim_cpg_order_method.dim_order_method_id where    
         
   (order_date_id>19000101 or ship_date_id>19000101) and     
   dim_item_id  not in (select distinct dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id in ('990-000-002-0','990-000-003-0')) and    
    src_channel_id='R' and src_order_status='IN'     
   and src_order_number  in (SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number  
                        FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock) where ltrim(rtrim(src_order_origin_code))='GR') 
   group by  dim_business_unit_id,dim_shop_site_id,src_currency_code_from, fact_cpg_sales_detail_kit_component.dim_order_method_id,  
            src_order_type,    
            dim_item_id,    
            src_order_number,    
            src_unit_cost,    
            src_kit_selling_price,    
            src_current_retail_price,    
            src_kit_units_ordered,    
            order_date_id     
   ) Tab_Kit_Items    
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id) Tab_Kit_Regular_Orders    
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id) Sub_Main    
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id)),
--[SP_Aggregate_Free_Orders]
#fact_aggregate_sales_temp3 as 
(select * from(
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
         --,current_date as create_timestamp
        --,'ETL' as created_by
        --,null as update_timestamp
        --,null as updated_by
        --,null as venue_key
        --,null as venue_flag
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
      , sum("Demand_Sales$") as "Demand_Sales$"  
      ,sum("Shipped_Sales$") as "Shipped_Sales$"  
      , sum("Return$") as "Return$"  
      , sum("Net_Sales_$") as "Net_Sales_$"  
      , sum("Demand_Selling_Margin$") as "Demand_Selling_Margin$"  
      , sum("Shipped_Selling_Margin$") as "Shipped_Selling_Margin$"  
       ,sum("Net_Selling_Margin$") as "Net_Selling_Margin$"  
FROM  
(  
/*Retrieving Demand sales for Regular Items*/  
            SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
                                     ,dim_order_method_id  
                                     ,case src_order_type when 'I' then 'F' else 'F' end as src_order_type         
             ,Date  
                                     ,dim_item_id  
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
                                     ,src_order_type         
             ,order_date_id as Date  
                                     ,dim_item_id  
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
                FROM "entdwdb"."fds_cpg"."fact_cpg_sales_detail" (nolock)  
                        left join
   "entdwdb"."fds_cpg"."dim_cpg_order_method" on fact_cpg_sales_detail.dim_order_method_id= dim_cpg_order_method.dim_order_method_id   where    
                        (order_date_id>19000101 or ship_date_id>19000101) and   
                        dim_item_id  not in (select distinct dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id in ('990-000-002-0','990-000-003-0')) and  
                        src_order_type='I' and src_channel_id<>'R' and  
                        dim_item_id not in
 (select distinct B.dim_item_id  from (select distinct B.dim_item_id as kit_id from "entdwdb"."fds_cpg"."dim_cpg_kit_item"(nolock) A 
inner join "entdwdb"."fds_cpg"."dim_cpg_item" (nolock) B  on A.src_kit_id=B.src_item_id
) A, "entdwdb"."fds_cpg"."dim_cpg_item"(NoLock) B 
where A.kit_id=B.dim_item_id )
                        and src_order_number  in (SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number  
                        FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock) where ltrim(rtrim(src_prepay_code))='F' and isnull(ltrim(rtrim(src_order_origin_code)),'AA')<>'GR')  
                        group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,fact_cpg_sales_detail.dim_order_method_id, src_order_type ,order_date_id,dim_item_id) Tab1  
                        group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id             
              UNION all  
/*Retrieving Shipped sales for Regular Items*/           
              SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
                                     ,dim_order_method_id  
                                     ,case src_order_type when 'I' then 'F' else 'F' end as src_order_type         
             ,Date  
                                     ,dim_item_id  
             ,Avg(src_unit_cost) as src_unit_cost  
                                     ,Avg(src_current_retail_price) as src_current_retail_price  
                                     ,Avg(src_selling_price) as src_selling_price  
                                     ,sum(src_units_ordered) as src_units_ordered  
                                     , SUM(src_units_shipped) as src_units_shipped  
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
            from (  
              SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
                                     ,fact_cpg_sales_detail.dim_order_method_id  
                                     ,src_order_type  
                                     ,ship_date_id as Date  
                                     ,dim_item_id                      
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
      FROM "entdwdb"."fds_cpg"."fact_cpg_sales_detail"(nolock)  
      left join
   "entdwdb"."fds_cpg"."dim_cpg_order_method" on fact_cpg_sales_detail.dim_order_method_id= dim_cpg_order_method.dim_order_method_id 			
      where  
      (order_date_id>19000101 or ship_date_id>19000101) and   
      dim_item_id  not in (select distinct dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id in ('990-000-002-0','990-000-003-0')) and  
                src_order_type='I' and ship_date_id is not null and src_channel_id<>'R' and  
                        dim_item_id not in
 (select distinct B.dim_item_id  from (select distinct B.dim_item_id as kit_id from "entdwdb"."fds_cpg"."dim_cpg_kit_item"(nolock) A 
inner join "entdwdb"."fds_cpg"."dim_cpg_item" (nolock) B  on A.src_kit_id=B.src_item_id
) A, "entdwdb"."fds_cpg"."dim_cpg_item"(NoLock) B 
where A.kit_id=B.dim_item_id )
                        and src_order_number  in (SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number  
                        FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock) where ltrim(rtrim(src_prepay_code))='F' and isnull(ltrim(rtrim(src_order_origin_code)),'AA')<>'GR')  
                        and src_order_status='IN'  
                        group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,fact_cpg_sales_detail.dim_order_method_id, src_order_type ,ship_date_id,dim_item_id) Tab2  
                        group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id  
            UNION ALL  
/*Retrieving Return sales for Regular Items*/             
SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
                                     ,dim_order_method_id  
                                    ,case src_order_type when 'C' then 'F' else 'F' end as src_order_type        
             , Date  
                                     ,dim_item_id  
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
            from (  
SELECT                        dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
                                    ,b.dim_order_method_id as dim_order_method_id  
                                    ,case src_order_type when 'C' then 'F' else 'F' end as src_order_type  
                                    ,order_date_id as Date  
                                    ,dim_item_id                      
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
From  
            (select dim_business_unit_id,dim_shop_site_id,src_currency_code_from,  
            case src_order_type when 'C' then 'F' else 'F' end as src_order_type ,  
            dim_item_id,  
            src_order_number,  
            src_unit_cost,  
            src_selling_price,  
            src_current_retail_price,  
            src_units_ordered,  
            order_date_id    
   FROM "entdwdb"."fds_cpg"."fact_cpg_sales_detail" (nolock) left join
   "entdwdb"."fds_cpg"."dim_cpg_order_method" on fact_cpg_sales_detail.dim_order_method_id= dim_cpg_order_method.dim_order_method_id    where      
   (order_date_id>19000101 or ship_date_id>19000101) and   
    dim_item_id  not in (select distinct dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id in ('990-000-002-0','990-000-003-0')) and  
    src_channel_id='R' and src_order_status='IN'  
   and dim_item_id not in
(select distinct B.dim_item_id  from (select distinct B.dim_item_id as kit_id from "entdwdb"."fds_cpg"."dim_cpg_kit_item"(nolock) A 
inner join "entdwdb"."fds_cpg"."dim_cpg_item" (nolock) B  on A.src_kit_id=B.src_item_id
) A, "entdwdb"."fds_cpg"."dim_cpg_item"(NoLock) B 
where A.kit_id=B.dim_item_id ) 
    and src_order_number  in (SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number  
    FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock) where ltrim(rtrim(src_prepay_code))='F' and isnull(ltrim(rtrim(src_order_origin_code)),'AA')<>'GR')  
   ) as A      
left outer join  
            (select B.src_order_number as src_order_number,  
            A.dim_order_method_id as dim_order_method_id   
            from "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock) A  
            inner join                         
                                    (select src_order_number,src_original_ref_order_number from "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock)  
                                                            left join
   "entdwdb"."fds_cpg"."dim_cpg_order_method" on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dim_order_method_id 	
where src_channel_id='R' ) B                                                              
                                                            on A.src_order_number=B.src_order_number   ) B  
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
      , sum("Demand_Sales$") as "Demand_Sales$"    
      ,sum("Shipped_Sales$") as "Shipped_Sales$"    
      , sum("Return$") as "Return$"    
      , sum("Net_Sales_$") as "Net_Sales_$"    
      , sum("Demand_Selling_Margin$") as "Demand_Selling_Margin$"    
      , sum("Shipped_Selling_Margin$") as "Shipped_Selling_Margin$"    
      ,sum("Net_Selling_Margin$") as "Net_Selling_Margin$"    
FROM    
/*Retrieving Demand sales for KIT Items*/    
(SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from    
         ,dim_order_method_id    
                                     ,src_order_type         
             ,Date    
                                     ,dim_item_id    
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
            from (     
            SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from    
                                     ,fact_cpg_sales_detail_kit_component.dim_order_method_id    
                                     ,src_order_type           
             ,order_date_id as Date    
                                     ,dim_item_id    
             ,Avg(src_unit_cost) as src_unit_cost    
                                     ,Avg(src_current_retail_price) as src_current_retail_price    
                                     ,Avg(((src_kit_selling_price*src_component_percent)/100)/(src_required_quantity)) as src_selling_price  --Has to Remove    
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
                FROM     
               "entdwdb"."fds_cpg"."fact_cpg_sales_detail_kit_component"(nolock)    
                                   left join
   "entdwdb"."fds_cpg"."dim_cpg_order_method" on fact_cpg_sales_detail_kit_component.dim_order_method_id= dim_cpg_order_method.dim_order_method_id
                                     where     
                                    (order_date_id>19000101 or ship_date_id>19000101) and     
                                    src_order_type='I'  and isnull(src_channel_id,'0')<>'R' and    
                                  src_order_number  in (SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number  
                        FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock) where ltrim(rtrim(src_prepay_code))='F' and isnull(ltrim(rtrim(src_order_origin_code)),'AA')<>'GR')      
            and src_order_number  in (    
            SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number    
            FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock)    
            where     
            ltrim(rtrim(src_original_ref_order_number))='0')     
         group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,fact_cpg_sales_detail_kit_component.dim_order_method_id, src_order_type ,order_date_id,dim_item_id,src_kit_selling_price) As A    
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id,src_order_type,Date,dim_item_id                   
              UNION all    
/*Retrieving Shipped sales for Kit Items*/        
             SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from    
                                     ,dim_order_method_id    
                                     ,src_order_type          
             ,Date    
                                     ,dim_item_id    
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
            from (    
 SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from    
                                     ,fact_cpg_sales_detail_kit_component.dim_order_method_id    
                                     ,src_order_type           
             ,ship_date_id as Date    
                                     ,dim_item_id    
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
                FROM     
                "entdwdb"."fds_cpg"."fact_cpg_sales_detail_kit_component"(nolock)    
                                   left join
   "entdwdb"."fds_cpg"."dim_cpg_order_method" on fact_cpg_sales_detail_kit_component.dim_order_method_id= dim_cpg_order_method.dim_order_method_id				
                                     where     
                                    (order_date_id>19000101 or ship_date_id>19000101) and     
                                    dim_item_id  not in (select distinct dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id in ('990-000-002-0','990-000-003-0')) and    
                                    src_order_type='I' and ship_date_id is not null and isnull(src_channel_id,'0')<>'R' 
                                    and src_order_number  in (SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number  
                        FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock) where ltrim(rtrim(src_prepay_code))='F' and isnull(ltrim(rtrim(src_order_origin_code)),'AA')<>'GR')  
                                    and src_order_status='IN'     
                                    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,fact_cpg_sales_detail_kit_component.dim_order_method_id, src_order_type ,ship_date_id,dim_item_id,src_kit_selling_price) As A    
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from    
      ,dim_order_method_id,src_order_type,Date,dim_item_id    
            UNION ALL    
/*Retrieving Return sales for Kit Items*/          
              SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from    
                                     ,dim_order_method_id    
                                     ,case src_order_type when 'C' then 'F' else 'F' end as src_order_type           
             ,Date    
                                     ,dim_item_id    
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
            from (    
SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from    
                                     ,fact_cpg_sales_detail_kit_component.dim_order_method_id    
                                    ,case src_order_type when 'C' then 'F' else 'F' end as src_order_type         
             ,order_date_id as Date    
                                     ,dim_item_id    
             ,Avg(src_unit_cost) as src_unit_cost    
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
                FROM     
"entdwdb"."fds_cpg"."fact_cpg_sales_detail_kit_component"(nolock)  left join
   "entdwdb"."fds_cpg"."dim_cpg_order_method" on fact_cpg_sales_detail_kit_component.dim_order_method_id= dim_cpg_order_method.dim_order_method_id where    
         
   (order_date_id>19000101 or ship_date_id>19000101) and     
   dim_item_id  not in (select distinct dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id in ('990-000-002-0','990-000-003-0')) and    
    src_channel_id='R' and src_order_status='IN' 
   and src_order_number  in (SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number  
                        FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"(nolock) where ltrim(rtrim(src_prepay_code))='F' and isnull(ltrim(rtrim(src_order_origin_code)),'AA')<>'GR')  
   group by  dim_business_unit_id,dim_shop_site_id,src_currency_code_from, fact_cpg_sales_detail_kit_component.dim_order_method_id,  
            src_order_type,    
            dim_item_id,    
            src_order_number,    
            src_unit_cost,    
            src_kit_selling_price,    
            src_current_retail_price,    
            src_kit_units_ordered,    
            order_date_id     
   ) Tab_Kit_Items    
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id) Tab_Kit_Regular_Orders    
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id) Sub_Main    
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id )),
--[SP_Aggregate_Header]
#fact_aggregate_sales_temp4 as 
(select *  from(
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
      -- ,current_date as create_timestamp
       -- ,'ETL' as created_by
        --,null as update_timestamp
        --,null as updated_by
        --,null as venue_key
        --,null as venue_flag
            from       
   (SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
     ,dim_order_method_id  
    ,src_order_type         
     ,order_date_id as Date_Key  
     ,(select dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id='TAX' and active_flag='Y') AS dim_item_id  
     ,Sum(src_sales_tax) Other_Amount  
     ,0 as SPECIALCHG  
    FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header" left join (select src_channel_id,dim_order_method_id as dm_order_method_id from "entdwdb"."fds_cpg"."dim_cpg_order_method") dim_cpg_order_method
	on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dm_order_method_id 
	where  
    order_date_id>19000101 and  
    src_order_type='I' and src_channel_id<>'R'  
    and (isnull(src_order_origin_code,'AA')<>'GR' or isnull(src_prepay_code,'A')<>'F')  
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,order_date_id  
   Union All  
    SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
     ,dim_order_method_id  
    ,case src_order_type when 'I'  then 'G' else 'G' end src_order_type         
     ,order_date_id as Date_Key  
     ,(select dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id='TAX' and active_flag='Y') AS dim_item_id  
     ,Sum(src_sales_tax) Other_Amount  
     ,0 as SPECIALCHG  
    FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header" left join (select src_channel_id,dim_order_method_id as dm_order_method_id from "entdwdb"."fds_cpg"."dim_cpg_order_method") dim_cpg_order_method
	on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dm_order_method_id 
	where  
    order_date_id>19000101 and  
    src_order_type='I' and src_channel_id<>'R'  
    and isnull(src_order_origin_code,'AA')='GR'  
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, case src_order_type when 'I'  then 'G' else 'G' end ,order_date_id  
   Union All  
    SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
     ,dim_order_method_id  
    ,case src_order_type when 'I'  then 'F' else 'F' end src_order_type  
     ,order_date_id as Date_Key  
     ,(select dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id='TAX' and active_flag='Y') AS dim_item_id  
     ,Sum(src_sales_tax) Other_Amount  
     ,0 as SPECIALCHG  
    FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header" 
    left join (select src_channel_id,dim_order_method_id as dm_order_method_id from "entdwdb"."fds_cpg"."dim_cpg_order_method") dim_cpg_order_method
	on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dm_order_method_id 
	where
    order_date_id>19000101 and  
    src_order_type='I' and src_channel_id<>'R'  
    and (isnull(src_prepay_code,'A')='F' and isnull(src_order_origin_code,'AA')<>'GR')  
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, case src_order_type when 'I'  then 'F' else 'F' end ,order_date_id  
   Union All  
    SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
     ,dim_order_method_id  
    ,src_order_type  
     ,order_date_id as Date_Key  
     ,(select dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id='TAX' and active_flag='Y') AS dim_item_id  
     ,Sum(src_sales_tax) Other_Amount  
     ,0 as SPECIALCHG  
    FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header" left join (select src_channel_id,dim_order_method_id as dm_order_method_id from "entdwdb"."fds_cpg"."dim_cpg_order_method") dim_cpg_order_method
	on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dm_order_method_id 
	where  
    order_date_id>19000101 and  
    src_channel_id='R'   
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,order_date_id  
    ) Tab_Tax  
group by  
  dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
      ,dim_order_method_id  
      ,src_order_type    
      ,Date_Key,  
      dim_item_id   
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
       --,current_date as create_timestamp
       -- ,'ETL' as created_by
       -- ,null as update_timestamp
       -- ,null as updated_by
        --,null as venue_key
        --,null as venue_flag
           from       
   (SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
     ,dim_order_method_id  
    ,src_order_type         
     ,order_date_id as Date_Key  
     ,(select dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id='SPECIALCHG' and active_flag='Y') AS dim_item_id  
    ,SUM(src_special_charges) Other_Amount  
     ,0 as SPECIALCHG  
    FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"
	left join (select src_channel_id,dim_order_method_id as dm_order_method_id from "entdwdb"."fds_cpg"."dim_cpg_order_method") dim_cpg_order_method
	on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dm_order_method_id 
	where  
    order_date_id>19000101 and  
    src_order_type='I' and src_channel_id<>'R'  
    and (isnull(src_order_origin_code,'AA')<>'GR' or isnull(src_prepay_code,'A')<>'F')  
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,order_date_id  
   Union All  
    SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
     ,dim_order_method_id  
    ,case src_order_type when 'I'  then 'G' else 'G' end src_order_type         
     ,order_date_id as Date_Key  
    ,(select dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id='SPECIALCHG' and active_flag='Y') AS dim_item_id  
    ,SUM(src_special_charges) Other_Amount  
     ,0 as SPECIALCHG  
    FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header" left join (select src_channel_id,dim_order_method_id as dm_order_method_id from "entdwdb"."fds_cpg"."dim_cpg_order_method") dim_cpg_order_method
	on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dm_order_method_id 
	where  
    order_date_id>19000101 and  
    src_order_type='I' and src_channel_id<>'R'  
    and isnull(src_order_origin_code,'AA')='GR'  
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, case src_order_type when 'I'  then 'G' else 'G' end ,order_date_id  
   Union All  
    SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
     ,dim_order_method_id  
    ,case src_order_type when 'I'  then 'F' else 'F' end src_order_type  
     ,order_date_id as Date_Key  
     ,(select dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id='SPECIALCHG' and active_flag='Y') AS dim_item_id  
    ,SUM(src_special_charges) Other_Amount  
     ,0 as SPECIALCHG  
    FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"
left join (select src_channel_id,dim_order_method_id as dm_order_method_id from "entdwdb"."fds_cpg"."dim_cpg_order_method") dim_cpg_order_method
	on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dm_order_method_id 
	where  
    order_date_id>19000101 and  
    src_order_type='I' and src_channel_id<>'R'  
    and (isnull(src_prepay_code,'A')='F' and isnull(src_order_origin_code,'AA')<>'GR')  
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, case src_order_type when 'I'  then 'F' else 'F' end ,order_date_id  
   Union All  
    SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
     ,dim_order_method_id  
    ,src_order_type  
     ,order_date_id as Date_Key  
     ,(select dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id='SPECIALCHG' and active_flag='Y') AS dim_item_id  
    ,SUM(src_special_charges) Other_Amount  
     ,0 as SPECIALCHG  
    FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"
	left join (select src_channel_id,dim_order_method_id as dm_order_method_id from "entdwdb"."fds_cpg"."dim_cpg_order_method") dim_cpg_order_method
	on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dm_order_method_id 
	where  
    order_date_id>19000101 and  
    src_channel_id='R'   
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,order_date_id  
    ) Tab_Tax  
group by  
  dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
      ,dim_order_method_id  
      ,src_order_type    
      ,Date_Key,  
      dim_item_id   
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
      -- ,current_date as create_timestamp
      --  ,'ETL' as created_by
      --  ,null as update_timestamp
      --  ,null as updated_by
      --  ,null as venue_key
      --  ,null as venue_flag
           from       
   (SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
     ,dim_order_method_id  
    ,src_order_type         
     ,order_date_id as Date_Key  
     ,(select dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id='FREIGHT' and active_flag='Y') AS dim_item_id  
     ,Sum(src_carrier_shipping_charges) Other_Amount  
     ,0 as SPECIALCHG  
    FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header" left join (select src_channel_id,dim_order_method_id as dm_order_method_id from "entdwdb"."fds_cpg"."dim_cpg_order_method") dim_cpg_order_method
	on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dm_order_method_id 
	
	where  
    order_date_id>19000101 and  
    src_order_type='I' and src_channel_id<>'R'  
    and (isnull(src_order_origin_code,'AA')<>'GR' or isnull(src_prepay_code,'A')<>'F')  
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,order_date_id  
   Union All  
    SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
     ,dim_order_method_id  
    ,case src_order_type when 'I'  then 'G' else 'G' end src_order_type         
     ,order_date_id as Date_Key  
    ,(select dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id='FREIGHT' and active_flag='Y') AS dim_item_id  
    ,Sum(src_carrier_shipping_charges) Other_Amount  
     ,0 as SPECIALCHG  
    FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header" 
	left join (select src_channel_id,dim_order_method_id as dm_order_method_id from "entdwdb"."fds_cpg"."dim_cpg_order_method") dim_cpg_order_method
	on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dm_order_method_id 
	where  
    order_date_id>19000101 and  
    src_order_type='I' and src_channel_id<>'R'  
    and isnull(src_order_origin_code,'AA')='GR'  
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, case src_order_type when 'I'  then 'G' else 'G' end ,order_date_id  
   Union All  
    SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
     ,dim_order_method_id  
    ,case src_order_type when 'I'  then 'F' else 'F' end src_order_type  
     ,order_date_id as Date_Key  
     ,(select dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id='FREIGHT' and active_flag='Y') AS dim_item_id  
    ,Sum(src_carrier_shipping_charges) Other_Amount  
     ,0 as SPECIALCHG  
    FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header" 
	left join (select src_channel_id,dim_order_method_id as dm_order_method_id from "entdwdb"."fds_cpg"."dim_cpg_order_method") dim_cpg_order_method
	on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dm_order_method_id 
	where  
    order_date_id>19000101 and  
    src_order_type='I' and src_channel_id<>'R'   
    and (isnull(src_prepay_code,'A')='F' and isnull(src_order_origin_code,'AA')<>'GR')  
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, case src_order_type when 'I'  then 'F' else 'F' end ,order_date_id  
   Union All  
    SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
     ,dim_order_method_id  
    ,src_order_type  
     ,order_date_id as Date_Key  
     ,(select dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_item" where src_item_id='FREIGHT' and active_flag='Y') AS dim_item_id  
    ,Sum(src_carrier_shipping_charges) Other_Amount  
     ,0 as SPECIALCHG  
    FROM "entdwdb"."fds_cpg"."fact_cpg_sales_header"
left join (select src_channel_id,dim_order_method_id as dm_order_method_id from "entdwdb"."fds_cpg"."dim_cpg_order_method") dim_cpg_order_method
	on fact_cpg_sales_header.dim_order_method_id= dim_cpg_order_method.dm_order_method_id 
	where  
    order_date_id>19000101 and  
    src_channel_id='R'   
    group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,order_date_id  
    ) Tab_Tax  
group by  dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id  ,src_order_type    ,Date_Key,  dim_item_id  )),

 #fact_aggregate_sales as
(select dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id,cast(src_order_type as varchar(1)) as src_order_type,date_key,dim_item_id,other_amount,src_unit_cost,src_current_retail_price,
src_units_ordered,src_units_shipped,units_returned,net_units_sold,demand_cogs$ demand_cogs_$,shipped_cogs$ shipped_cogs_$,
returned_cogs$ returned_cogs_$,net_cogs$ net_cogs_$,demand_retail$ demand_retail_$,shipped_retail$ shipped_retail_$,
net_retail$ net_retail_$,demand_sales$ demand_sales_$,shipped_sales$ shipped_sales_$,return$ returns_$,
net_sales_$,demand_selling_margin$ demand_selling_margin_$,shipped_selling_margin$ shipped_selling_margin_$
,net_selling_margin$ net_selling_margin_$
 from
(
 select * from #fact_aggregate_sales_temp1 
		union all
		select * from #fact_aggregate_sales_temp2
		union all
		select * from #fact_aggregate_sales_temp3
		union all
		select * from #fact_aggregate_sales_temp4
)),
#fact_aggregate_sales_final as 
(select * from  #fact_aggregate_sales where (src_currency_code_from!='CAD' or date_key !='19000101'))
select dim_business_unit_id,
dim_order_method_id,
date_key,
src_order_type,
dim_item_id,
src_unit_cost,
src_current_retail_price,
src_units_ordered,
src_units_shipped,
units_returned,
net_units_sold,
demand_cogs_$,
shipped_cogs_$,
returned_cogs_$,
net_cogs_$,
demand_retail_$,
shipped_retail_$,
net_retail_$,
demand_sales_$,
shipped_sales_$,
returns_$,
net_sales_$,
demand_selling_margin_$,
shipped_selling_margin_$,
net_selling_margin_$,
other_amount,
current_timestamp as create_timestamp,
'ETL' as created_by,
null as update_timestamp,
null as updated_by,
(src_unit_cost/nullif(conversion_rate_to_local,0)) as unit_cost_local,
(src_current_retail_price/nullif(conversion_rate_to_local,0)) as current_retail_price_local,
(demand_cogs_$/nullif(conversion_rate_to_local,0)) as demand_cogs_local,
(shipped_cogs_$/nullif(conversion_rate_to_local,0)) as shipped_cogs_local,
(returned_cogs_$/nullif(conversion_rate_to_local,0)) as returned_cogs_local,
(net_cogs_$/nullif(conversion_rate_to_local,0)) as net_cogs_local,
(demand_retail_$/nullif(conversion_rate_to_local,0)) as demand_retail_local,
(shipped_retail_$/nullif(conversion_rate_to_local,0)) as shipped_retail_local,
(net_retail_$/nullif(conversion_rate_to_local,0)) as net_retail_local,
(demand_sales_$/nullif(conversion_rate_to_local,0)) as demand_sales_local,
(shipped_sales_$/nullif(conversion_rate_to_local,0)) as shipped_sales_local,
(returns_$/nullif(conversion_rate_to_local,0)) as returns_local,
(net_sales_$/nullif(conversion_rate_to_local,0)) as net_sales_local,
(demand_selling_margin_$/nullif(conversion_rate_to_local,0)) as demand_selling_margin_local,
(shipped_selling_margin_$/nullif(conversion_rate_to_local,0)) as shipped_selling_margin_local,
(net_selling_margin_$/nullif(conversion_rate_to_local,0)) as net_selling_margin_local,
(other_amount/nullif(conversion_rate_to_local,0)) as other_amount_local,
src_currency_code_from,
dim_shop_site_id,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_FDS_CPG' as etl_batch_id, 'bi_dbt_user_prd' as etl_insert_user_id, 
current_timestamp as etl_insert_rec_dttm, null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from 
(
	select src.*,
	coalesce((case when src.src_currency_code_from='USD' then 1 else cc.currency_conversion_rate_spot_rate end),0) as conversion_rate_to_local
	from #fact_aggregate_sales_final src
	--left outer join public.dim_Date dt on  dt.datekey=src.date_key
	left outer join (select * from dt_stage.prestg_cpg_currency_exchange_rate ) cc
	on cast(cast(src.date_key as varchar(20)) as date) =cc.as_on_date and src.src_currency_code_from=cc.currency_code_from and cc.currency_code_to='USD'
)