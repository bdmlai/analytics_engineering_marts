{{
  config({
		"materialized": 'ephemeral'
  })
}}
--Kit into Components for Gratis Orders--
Select dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
  ,dim_order_method_id
  ,case src_order_type when 'C' then 'GR' else 'GR' end as src_order_type  
  ,Date as Date_Key
  ,dim_item_id
  ,dim_src_kit_item_id
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
  ,sum("Net$") as "Net$"
  ,sum("Demand_Selling_Margin$") as "Demand_Selling_Margin$"
  ,sum("Shipped_Selling_Margin$") as "Shipped_Selling_Margin$"
  ,sum("Net_Selling_Margin$") as "Net_Selling_Margin$"
FROM
/*Retrieving Demand sales for KIT Items*/
(SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
								 ,dim_order_method_id
								 ,case src_order_type when 'I' then 'GR' else 'GR' end as src_order_type     
								 ,Date
								 ,dim_item_id
								 ,dim_src_kit_item_id
								 ,Avg(src_unit_cost) as src_unit_cost
								 ,Avg(src_current_retail_price) as src_current_retail_price
								 ,Avg(src_selling_price) as src_selling_price
								 ,sum(src_units_ordered*src_required_quantity) as src_units_ordered
								 ,SUM(src_units_shipped*src_required_quantity) as src_units_shipped
								 ,SUM(Units_Returned*src_required_quantity) as Units_Returned
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
  ,sum("Net$") as "Net$"
  ,sum(isnull("Demand_Sales$",0)-isnull("Demand_Cogs$",0)) as "Demand_Selling_Margin$"
  ,sum(isnull("Shipped_Sales$",0)-isnull("Shipped_Cogs$",0)) as "Shipped_Selling_Margin$"
  ,sum(isnull("Net$",0)-isnull("Net_Cogs$",0)) as "Net_Selling_Margin$"
	from ( 
	SELECT DISTINCT
    dim_business_unit_id,
    dim_shop_site_id,
    src_currency_code_from ,
    dim_order_method_id ,
    src_order_type ,
    DATE ,
    B.dim_item_id AS dim_item_id ,
    A.dim_item_id AS dim_src_kit_item_id,
    AVG(src_unit_cost)                                                 AS src_unit_cost ,
    AVG(src_current_retail_price)                                      AS src_current_retail_price ,
    AVG(((src_selling_price*src_component_percent_of_price)/100)/nullif(B.src_required_quantity,0)) AS
    src_selling_price
    ,
    SUM(src_units_ordered)                                              AS src_units_ordered ,
    0                                                                   AS src_units_shipped ,
    0                                                                      AS Units_Returned ,
    SUM(B.src_required_quantity)                                          AS src_required_quantity ,
    SUM(ISNULL(src_units_ordered,0)*B.src_required_quantity *src_unit_cost)      AS "Demand_Cogs$" ,
    0                                                                           AS "Shipped_Cogs$" ,
    0                                                                          AS "Returned_Cogs$" ,
    0                                                                               AS "Net_Cogs$" ,
    SUM(src_units_ordered*B.src_required_quantity * ISNULL(src_current_retail_price,0)) AS
         "Demand_Retail$" ,
    0                                                                         AS "Shipped_Retail$" ,
    0                                                                             AS "Net_Retail$" ,
    SUM(src_units_ordered * ((ISNULL(src_selling_price,0)*src_component_percent_of_price)/100)) AS
         "Demand_Sales$" ,
    0 AS "Shipped_Sales$" ,
    0 AS "Return$" ,
    0 AS "Net$"
FROM
    (
        SELECT DISTINCT
            fact_cpg_sales_detail.dim_business_unit_id,
            dim_shop_site_id,
            src_currency_code_from ,
            fact_cpg_sales_detail.dim_order_method_id ,
            src_order_type ,
            order_date_id AS DATE ,
            fact_cpg_sales_detail.dim_item_id ,
            src_selling_price ,
            SUM(src_units_ordered) AS src_units_ordered
        FROM
            {{source('fds_cpg','fact_cpg_sales_detail')}}
		LEFT JOIN
            {{source('fds_cpg','dim_cpg_order_method')}}
        ON
            fact_cpg_sales_detail.dim_order_method_id=
            dim_cpg_order_method.dim_order_method_id
        WHERE
            src_order_type='I'
        AND ISNULL(src_channel_id,'0')<>'R'
        AND fact_cpg_sales_detail.dim_item_id IN
            (
                SELECT DISTINCT
                   A.dim_src_kit_item_id
                FROM
                    {{source('fds_cpg','dim_cpg_kit_item')}} A,
                    {{source('fds_cpg','dim_cpg_item')}} B
                WHERE
                    A.dim_src_kit_item_id=B.dim_item_id)
					AND src_order_number IN
                                 (
                                 SELECT DISTINCT
                                     ltrim(RTRIM(src_order_number)) AS src_order_number
                                 FROM
                                     {{source('fds_cpg','fact_cpg_sales_header')}}
                                 WHERE
                                     ltrim(RTRIM(src_order_origin_code))='GR')
        GROUP BY
            fact_cpg_sales_detail.dim_business_unit_id,
            dim_shop_site_id,
            src_currency_code_from ,
            fact_cpg_sales_detail.dim_order_method_id,
            src_order_type ,
            order_date_id,
            fact_cpg_sales_detail.dim_item_id,
            src_selling_price) AS A
LEFT OUTER JOIN
    (
        SELECT
            k.dim_src_kit_item_id,
			c.dim_item_id,
            AVG(ISNULL(C.src_unit_cost,0))            AS src_unit_cost ,
            AVG(ISNULL(C.src_current_retail_price,0)) AS src_current_retail_price ,
            AVG(src_component_percent_of_price)       AS src_component_percent_of_price ,
            AVG(src_required_quantity)                AS src_required_quantity
        FROM
            (
                SELECT
					dim_src_kit_item_id,
					src_kit_id,
					a.dim_item_id, 
                    src_required_quantity ,
                    src_component_percent_of_price AS src_component_percent_of_price
                FROM
                    {{source('fds_cpg','dim_cpg_kit_item')}} A
				INNER JOIN
                    {{source('fds_cpg','dim_cpg_item')}} B
                ON
                    a.dim_src_kit_item_id=b.dim_item_id ) k
        LEFT OUTER JOIN
            {{source('fds_cpg','dim_cpg_item')}} c
        ON
           k.dim_item_id=c.dim_item_id
        GROUP BY
            k.dim_src_kit_item_id,
			c.dim_item_id) B
ON
      A.dim_item_id=B.dim_src_kit_item_id
GROUP BY
    dim_business_unit_id,
    dim_shop_site_id,
    src_currency_code_from ,
    dim_order_method_id,
    src_order_type,
    DATE,
    B.dim_item_id,
    A.dim_item_id) Tab4
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id,src_order_type,Date,dim_item_id,dim_src_kit_item_id       
	UNION all
/*Retrieving Shipped sales for Kit Items*/    
	SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
								 ,dim_order_method_id
								 ,case src_order_type when 'I' then 'GR' else 'GR' end as src_order_type      
								 ,Date
								 ,dim_item_id
								 ,dim_src_kit_item_id
								 ,Avg(src_unit_cost) as src_unit_cost
								 ,Avg(src_current_retail_price) as src_current_retail_price
								 ,Avg(src_selling_price) as src_selling_price
								 ,sum(src_units_ordered*src_required_quantity) as src_units_ordered
								 ,SUM(src_units_shipped*src_required_quantity) as src_units_shipped
								 ,SUM(Units_Returned*src_required_quantity) as Units_Returned
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
  ,sum("Net$") as "Net$"
  ,sum(isnull("Demand_Sales$",0)-isnull("Demand_Cogs$",0)) as "Demand_Selling_Margin$"
  ,sum(isnull("Shipped_Sales$",0)-isnull("Shipped_Cogs$",0)) as "Shipped_Selling_Margin$"
  ,sum(isnull("Net$",0)-isnull("Net_Cogs$",0)) as "Net_Selling_Margin$"
	from (SELECT DISTINCT
    dim_business_unit_id,
    dim_shop_site_id,
    src_currency_code_from ,
    dim_order_method_id ,
    src_order_type ,
    DATE ,
    B.dim_item_id AS dim_item_id ,
    A.dim_item_id AS dim_src_kit_item_id ,
    AVG(src_unit_cost)                                                 AS src_unit_cost ,
    AVG(src_current_retail_price)                                      AS src_current_retail_price ,
    AVG(((src_selling_price*src_component_percent_of_price)/100)/nullif(B.src_required_quantity,0)) AS
         src_selling_price ,
    0                                              AS src_units_ordered ,
    SUM(src_units_shipped)                                              AS src_units_shipped ,
    0                                                                      AS Units_Returned ,
    SUM(B.src_required_quantity)                                          AS src_required_quantity ,
    0                                                                       AS "Demand_Cogs$" ,
    SUM (src_units_shipped*B.src_required_quantity*ISNULL(src_unit_cost,0))     AS "Shipped_Cogs$" ,
    0                                                                          AS "Returned_Cogs$" ,
    SUM (src_units_shipped*B.src_required_quantity*ISNULL(src_unit_cost,0))         AS "Net_Cogs$" ,
    0                                                                          AS "Demand_Retail$" ,
    SUM (src_units_shipped*B.src_required_quantity* ISNULL(src_current_retail_price,0)) AS
    "Shipped_Retail$" ,
    SUM ((src_units_shipped)*B.src_required_quantity *ISNULL(src_current_retail_price,0)) AS
         "Net_Retail$" ,
    0                                                                           AS "Demand_Sales$" ,
    SUM(src_units_shipped*((ISNULL(src_selling_price,0)*src_component_percent_of_price)/100)) AS
         "Shipped_Sales$" ,
    0                                                                                 AS "Return$" ,
    SUM(src_units_shipped*((ISNULL(src_selling_price,0)*src_component_percent_of_price)/100)) AS
    "Net$"
FROM
    (
        SELECT DISTINCT
            fact_cpg_sales_detail.dim_business_unit_id,
            dim_shop_site_id,
            src_currency_code_from ,
            fact_cpg_sales_detail.dim_order_method_id ,
            src_order_type ,
            Ship_Date_id AS DATE ,
            fact_cpg_sales_detail.dim_item_id ,
            src_selling_price ,
            SUM(src_units_shipped) AS src_units_shipped
        FROM
            {{source('fds_cpg','fact_cpg_sales_detail')}}
		LEFT JOIN
            {{source('fds_cpg','dim_cpg_order_method')}}
        ON
            fact_cpg_sales_detail.dim_order_method_id=
            dim_cpg_order_method.dim_order_method_id
        WHERE
            src_order_type='I'
        AND Ship_Date_id IS NOT NULL
        AND ISNULL(src_channel_id,'0')<>'R'
        AND fact_cpg_sales_detail.dim_item_id IN
            (
                SELECT DISTINCT
                    B.dim_item_id
                FROM
                    {{source('fds_cpg','dim_cpg_kit_item')}} A,
                    {{source('fds_cpg','dim_cpg_item')}} B
                WHERE
                    A.src_kit_id=B.src_item_id )
        AND src_order_number IN
                                 (
                                 SELECT DISTINCT
                                     ltrim(RTRIM(src_order_number)) AS src_order_number
                                 FROM
                                     {{source('fds_cpg','fact_cpg_sales_header')}}
                                 WHERE
                                     ltrim(RTRIM(src_order_origin_code))='GR')
        AND src_Order_Status='IN'
        GROUP BY
            fact_cpg_sales_detail.dim_business_unit_id,
            dim_shop_site_id,
            src_currency_code_from ,
            fact_cpg_sales_detail.dim_order_method_id,
            src_order_type ,
            Ship_Date_id,
            fact_cpg_sales_detail.dim_item_id,
            src_selling_price) AS A
LEFT OUTER JOIN
    (
        SELECT
            k.dim_item_id,
			dim_src_kit_item_id,                        
            AVG(ISNULL(C.src_unit_cost,0))            AS src_unit_cost ,
            AVG(ISNULL(C.src_current_retail_price,0)) AS src_current_retail_price ,
            AVG(src_component_percent_of_price)       AS src_component_percent_of_price ,
            AVG(src_required_quantity)                AS src_required_quantity
        FROM
            (
                SELECT
                    dim_src_kit_item_id, 
					a.dim_item_id,
                    src_required_quantity ,
                    src_component_percent_of_price AS src_component_percent_of_price
                FROM
                    {{source('fds_cpg','dim_cpg_kit_item')}} A
				INNER JOIN
                    {{source('fds_cpg','dim_cpg_kit_item')}} B
                ON
                   a.dim_src_kit_item_id=b.dim_item_id ) k
        LEFT OUTER JOIN
            {{source('fds_cpg','dim_cpg_item')}} c
        ON
           k.dim_item_id=c.dim_item_id
        GROUP BY
            dim_src_kit_item_id,   
			k.dim_item_id) B
ON
    A.dim_item_id=B.dim_src_kit_item_id
GROUP BY
    dim_business_unit_id,
    dim_shop_site_id,
    src_currency_code_from ,
    dim_order_method_id,
    src_order_type,
    DATE,
    B.dim_item_id,
   A.dim_item_id) Tab5
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id,src_order_type,Date,dim_item_id,dim_src_kit_item_id
	UNION ALL
/*Retrieving Return sales for Kit Items*/      
	SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
								 ,dim_order_method_id
								 ,case src_order_type when 'C' then 'GR' else 'GR' end as src_order_type       
								 ,Date
								 ,dim_item_id
								 ,dim_src_kit_item_id
								 ,Avg(src_unit_cost) as src_unit_cost
								 ,Avg(src_current_retail_price) as src_current_retail_price
								 ,Avg(src_selling_price) as src_selling_price
								 ,sum(src_units_ordered*src_required_quantity) as src_units_ordered
								 ,SUM(src_units_shipped*src_required_quantity) as src_units_shipped
								 ,SUM(Units_Returned*src_required_quantity) as Units_Returned
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
  ,sum("Net$") as "Net$"
  ,sum(isnull("Demand_Sales$",0)-isnull("Demand_Cogs$",0)) as "Demand_Selling_Margin$"
  ,sum(isnull("Shipped_Sales$",0)-isnull("Shipped_Cogs$",0)) as "Shipped_Selling_Margin$"
  ,sum(isnull("Net$",0)-isnull("Net_Cogs$",0)) as "Net_Selling_Margin$"
	from (
SELECT DISTINCT
    dim_business_unit_id,
    dim_shop_site_id,
    src_currency_code_from ,
    dim_order_method_id ,
    CASE src_order_type
        WHEN 'C'
        THEN 'GR'
        ELSE 'GR'
    END AS src_order_type ,
    DATE ,
    B.dim_item_id AS dim_item_id 
   ,A.dim_item_id AS dim_src_kit_item_id
   ,AVG(src_unit_cost)                                                 AS src_unit_cost ,
    AVG(src_current_retail_price)                                      AS src_current_retail_price ,
    AVG(((src_selling_price*src_component_percent_of_price)/100)/nullif(B.src_required_quantity,0)) AS
         src_selling_price ,
    0                                              AS src_units_ordered ,
    0                                              AS src_units_shipped ,
    SUM(src_units_ordered)                                                 AS Units_Returned ,
    SUM(B.src_required_quantity)                                          AS src_required_quantity ,
    0                                                                      AS "Demand_Cogs$" ,
    0                                                                      AS "Shipped_Cogs$" ,
    SUM(src_units_ordered*B.src_required_quantity*ISNULL(src_unit_cost,0))     AS "Returned_Cogs$" ,
    SUM(src_units_ordered*B.src_required_quantity*ISNULL(src_unit_cost,0))          AS "Net_Cogs$" ,
    0                                                                          AS "Demand_Retail$" ,
    0                                                                         AS "Shipped_Retail$" ,
    SUM ((src_units_ordered)*B.src_required_quantity *ISNULL(src_current_retail_price,0)) AS
         "Net_Retail$" ,
    0                                                                           AS "Demand_Sales$" ,
    0                                                                          AS "Shipped_Sales$" ,
    SUM((src_units_ordered * ISNULL(src_selling_price,0)*src_component_percent_of_price)/100) AS
    "Return$" ,
    SUM(src_units_ordered* ((ISNULL(src_selling_price,0)*src_component_percent_of_price)/100)) AS
    "Net$"
FROM
    (
        SELECT DISTINCT
            dim_business_unit_id,
            dim_shop_site_id,
            src_currency_code_from ,
            b.dim_order_method_id AS dim_order_method_id ,
            CASE src_order_type
                WHEN 'C'
                THEN 'GR'
                ELSE 'GR'
            END           AS src_order_type ,
            order_date_id AS DATE ,
            dim_item_id ,
            src_selling_price ,
            SUM(src_units_ordered) AS src_units_ordered
        FROM
            (
                SELECT
                    fact_cpg_sales_detail.dim_business_unit_id,
                    dim_shop_site_id,
                    src_currency_code_from ,
                    CASE src_order_type
                        WHEN 'C'
                        THEN 'GR'
                        ELSE 'GR'
                    END AS src_order_type ,
                    fact_cpg_sales_detail.dim_item_id,
                    src_order_number,
                    fact_cpg_sales_detail.src_unit_cost,
                    fact_cpg_sales_detail.src_selling_price,
                    fact_cpg_sales_detail.src_current_retail_price,
                    src_units_ordered,
                    order_date_id
                FROM
                    {{source('fds_cpg','fact_cpg_sales_detail')}}
				LEFT JOIN
					{{source('fds_cpg','fact_cpg_sales_detail')}}
				ON
            fact_cpg_sales_detail.dim_order_method_id=
            dim_cpg_order_method.dim_order_method_id
                WHERE
               ISNULL(src_channel_id,'0')='R' AND src_Order_Status='IN'
                AND fact_cpg_sales_detail.dim_item_id IN
                    (
                        SELECT DISTINCT
                                A.dim_src_kit_item_id
                        FROM
                            {{source('fds_cpg','fact_cpg_sales_detail')}} A,
                            {{source('fds_cpg','dim_cpg_item')}} B
                        WHERE
                            A.dim_src_kit_item_id=B.dim_item_id)
							AND src_order_number IN
                                         (
                                         SELECT DISTINCT
                                             ltrim(RTRIM(src_order_number)) AS src_order_number
                                         FROM
                                             {{source('fds_cpg','fact_cpg_sales_header')}}
                                         WHERE
                                             ltrim(RTRIM(src_order_origin_code))='GR')
                GROUP BY
                    fact_cpg_sales_detail.dim_business_unit_id,
                    dim_shop_site_id,
                    src_currency_code_from ,
                    src_order_type,
					fact_cpg_sales_detail.dim_item_id,
                    src_order_number,
                    fact_cpg_sales_detail.src_unit_cost,
                    fact_cpg_sales_detail.src_selling_price,
                    fact_cpg_sales_detail.src_current_retail_price,
                    src_units_ordered,
                    order_date_id ) AS A
        LEFT OUTER JOIN
            (
                SELECT
                    B.src_order_number    AS src_order_number,
                    A.dim_order_method_id AS dim_order_method_id
                FROM
                    {{source('fds_cpg','fact_cpg_sales_header')}} A
                INNER JOIN
                    (
                        SELECT
                            src_order_number,
                            src_original_ref_order_number
                        FROM
                            {{source('fds_cpg','fact_cpg_sales_header')}}
                        WHERE
                            dim_order_method_id in (select dim_order_method_id from {{source('fds_cpg','dim_cpg_order_method')}}
							where ISNULL(src_channel_id,'0')='R')) B
                ON
                    A.src_order_number=B.src_order_number ) B
        ON
            A.src_order_number=B.src_order_number
        GROUP BY
            dim_business_unit_id,
            dim_shop_site_id,
            src_currency_code_from ,
            b.dim_order_method_id,
            src_order_type ,
            order_date_id,
            dim_item_id,
            src_selling_price ) AS A
LEFT OUTER JOIN
    (
        SELECT
            k.dim_src_kit_item_id,
			c.dim_item_id,
            AVG(ISNULL(C.src_unit_cost,0))            AS src_unit_cost ,
            AVG(ISNULL(C.src_current_retail_price,0)) AS src_current_retail_price ,
            AVG(src_component_percent_of_price)       AS src_component_percent_of_price ,
            AVG(src_required_quantity)                AS src_required_quantity
        FROM
            (
                SELECT   
					dim_src_kit_item_id,
					a.dim_item_id,
                    src_required_quantity ,
                    AVG(src_component_percent_of_price) AS src_component_percent_of_price
                FROM
                      {{source('fds_cpg','dim_cpg_kit_item')}} A
				INNER JOIN
                    {{source('fds_cpg','dim_cpg_kit_item')}} B
                ON
                  a.dim_src_kit_item_id=b.dim_item_id
                GROUP BY
                    dim_src_kit_item_id,
					a.dim_item_id,
                    src_required_quantity) k
        LEFT OUTER JOIN
            {{source('fds_cpg','dim_cpg_item')}} c
        ON
            k.dim_item_id=c.dim_item_id
        GROUP BY
               k.dim_src_kit_item_id,
			   c.dim_item_id) B
ON
    A.dim_item_id=B.dim_src_kit_item_id 
GROUP BY
    dim_business_unit_id,
    dim_shop_site_id,
    src_currency_code_from ,
    dim_order_method_id,
    src_order_type,
    DATE,
    B.dim_item_id,
    A.dim_item_id) Tab_Kit_Items
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id,dim_src_kit_item_id) Tab_Kit_Gratis_Orders
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id,dim_src_kit_item_id