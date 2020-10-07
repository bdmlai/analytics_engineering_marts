
with __dbt__CTE__intm_cpg_kit_sales_regular as (

--Kit into Components for Regular Items---
Select dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
  ,dim_order_method_id
  ,case src_order_type when 'C' then 'I' else 'I' end as src_order_type  
  ,Date as  Date_Key
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
--Demand--
(SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
  ,dim_order_method_id,src_order_type,Date,dim_item_id,dim_src_kit_item_id
  ,Avg(src_unit_cost) as src_unit_cost
  ,Avg(src_current_retail_price) as src_current_retail_price
  ,Avg(src_selling_price) as src_selling_price
  ,sum(src_units_ordered*src_required_quantity) as src_units_ordered
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
    ,SUM(src_units_ordered*src_required_quantity)                            AS src_units_ordered ,
    0                                                                       AS src_units_shipped ,
    0                                                                       AS Units_Returned ,
    SUM(B.src_required_quantity)                                          AS src_required_quantity ,
    SUM(src_units_ordered*B.src_required_quantity *ISNULL(src_unit_cost,0))      AS "Demand_Cogs$" ,
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
            fact_cpg_sales_detail.dim_item_id,
            src_selling_price ,
            SUM(src_units_ordered) AS src_units_ordered
        FROM
            "entdwdb"."fds_cpg"."fact_cpg_sales_detail"
		LEFT JOIN
            "entdwdb"."fds_cpg"."fact_cpg_sales_detail"
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
                                "entdwdb"."fds_cpg"."dim_cpg_kit_item" A,
                                "entdwdb"."fds_cpg"."dim_cpg_item" B
                            WHERE
                                A.dim_src_kit_item_id=B.dim_item_id )
									AND src_order_number NOT IN
                                     (
                                     SELECT DISTINCT
                                         ltrim(RTRIM(src_order_number)) AS src_order_number
                                     FROM
                                         "entdwdb"."fds_cpg"."fact_cpg_sales_header"
                                     WHERE
                                         ltrim(RTRIM(src_order_origin_code))='GR'
										OR  ltrim(RTRIM(src_prepay_code))='F')
										AND src_order_number IN
                                 (
                                 SELECT DISTINCT
                                     ltrim(RTRIM(src_order_number)) AS src_order_number
                                 FROM
                                     "entdwdb"."fds_cpg"."fact_cpg_sales_header"
                                 WHERE
                                     ltrim(RTRIM(src_original_ref_order_number))='0')
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
    (src_component_percent_of_price)       AS src_component_percent_of_price ,
    (src_required_quantity)                AS src_required_quantity
                FROM
                    "entdwdb"."fds_cpg"."dim_cpg_kit_item" A
				INNER JOIN
                    "entdwdb"."fds_cpg"."dim_cpg_item" B
                ON
                   a.dim_src_kit_item_id=b.dim_item_id
) k
        LEFT OUTER JOIN
            "entdwdb"."fds_cpg"."dim_cpg_item" c
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
								 ,src_order_type      
								 ,Date
								 ,dim_item_id
								 ,dim_src_kit_item_id
								 ,Avg(src_unit_cost) as src_unit_cost
								 ,Avg(src_current_retail_price) as src_current_retail_price
								 ,Avg(src_selling_price) as src_selling_price
								 ,sum(src_units_ordered*src_required_quantity) as src_units_ordered
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
            AVG(src_unit_cost)            AS src_unit_cost ,
            AVG(src_current_retail_price) AS src_current_retail_price ,
            AVG(((src_selling_price*src_component_percent_of_price)/100)/nullif(B.src_required_quantity,0))
                                                         AS src_selling_price ,
            0                                            AS src_units_ordered ,
            SUM(src_units_shipped*src_required_quantity) AS src_units_shipped ,
            0                                                                    AS Units_Returned ,
            SUM(B.src_required_quantity)                                  AS src_required_quantity ,
            0                                                                    AS "Demand_Cogs$" ,
            SUM (src_units_shipped*B.src_required_quantity*ISNULL(src_unit_cost,0)) AS
                                                                                   "Shipped_Cogs$" ,
            0                                                                  AS "Returned_Cogs$" ,
            SUM (src_units_shipped*B.src_required_quantity*ISNULL(src_unit_cost,0)) AS "Net_Cogs$"
            ,
            0                                                                  AS "Demand_Retail$" ,
            SUM (src_units_shipped*B.src_required_quantity* ISNULL(src_current_retail_price,0)) AS
            "Shipped_Retail$" ,
            SUM ((src_units_shipped)*B.src_required_quantity *ISNULL(src_current_retail_price,0))
              AS "Net_Retail$" ,
            0 AS "Demand_Sales$" ,
            SUM(src_units_shipped*((ISNULL(src_selling_price,0)*src_component_percent_of_price)/100
            )) AS "Shipped_Sales$" ,
            0  AS "Return$" ,
            SUM(src_units_shipped*((ISNULL(src_selling_price,0)*src_component_percent_of_price)/100
            )) AS "Net$"
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
                    "entdwdb"."fds_cpg"."fact_cpg_sales_detail"
				LEFT JOIN
					"entdwdb"."fds_cpg"."dim_cpg_order_method"
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
                              A.dim_src_kit_item_id
                        FROM
                            "entdwdb"."fds_cpg"."dim_cpg_kit_item" A,
                            "entdwdb"."fds_cpg"."dim_cpg_item" B
                        WHERE
                        A.dim_src_kit_item_id=B.dim_item_id )
						AND src_order_number NOT IN
                                             (
                                             SELECT DISTINCT
                                                 ltrim(RTRIM(src_order_number)) AS src_order_number
                                             FROM
                                                 "entdwdb"."fds_cpg"."fact_cpg_sales_header"
                                             WHERE
                                                 ltrim(RTRIM(src_order_origin_code))='GR'
                                             OR  ltrim(RTRIM(src_prepay_code))='F')
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
				 (src_component_percent_of_price)       AS src_component_percent_of_price ,
		         (src_required_quantity)                AS src_required_quantity
                FROM
                    "entdwdb"."fds_cpg"."dim_cpg_kit_item" A
					INNER JOIN
                    "entdwdb"."fds_cpg"."dim_cpg_item" B
                ON
                  a.dim_src_kit_item_id=b.dim_item_id
) k
        LEFT OUTER JOIN
            "entdwdb"."fds_cpg"."dim_cpg_item" c
        ON
              k.dim_item_id=c.dim_item_id
        GROUP BY
           k.dim_src_kit_item_id,c.dim_item_id) B
ON
   A.dim_item_id=B.dim_src_kit_item_id
    GROUP BY dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id,src_order_type,DATE,B.dim_item_id,A.dim_item_id) Tab5
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id,src_order_type,Date,dim_item_id,dim_src_kit_item_id
	UNION ALL
/*Retrieving Return sales for Kit Items*/      
SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
  ,dim_order_method_id
  ,case src_order_type when 'C' then 'I' else 'I' end as src_order_type       
  ,Date,dim_item_id,dim_src_kit_item_id
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
        THEN 'I'
        ELSE 'I'
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
                THEN 'I'
                ELSE 'I'
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
                        THEN 'I'
                        ELSE 'I'
                    END AS src_order_type ,
                    fact_cpg_sales_detail.dim_item_id,
                    src_order_number,
                    fact_cpg_sales_detail.src_unit_cost,
                    fact_cpg_sales_detail.src_selling_price,
                    fact_cpg_sales_detail.src_current_retail_price,
                    src_units_ordered,
                    order_date_id
                FROM
                    "entdwdb"."fds_cpg"."fact_cpg_sales_detail"
		LEFT JOIN
            "entdwdb"."fds_cpg"."dim_cpg_order_method"
        ON
            fact_cpg_sales_detail.dim_order_method_id=
            dim_cpg_order_method.dim_order_method_id
                WHERE
				ISNULL(src_channel_id,'0')='R'
                AND src_Order_Status='IN'
                AND fact_cpg_sales_detail.dim_item_id IN
                    (
                        SELECT DISTINCT
                            A.dim_src_kit_item_id
                        FROM
                            "entdwdb"."fds_cpg"."dim_cpg_kit_item" A,
                            "entdwdb"."fds_cpg"."dim_cpg_item" B
                        WHERE
                           A.dim_src_kit_item_id=B.dim_item_id )
							AND src_order_number NOT IN
                                             (
                                             SELECT DISTINCT
                                                 ltrim(RTRIM(src_order_number)) AS src_order_number
                                             FROM
                                                 "entdwdb"."fds_cpg"."fact_cpg_sales_header"
                                             WHERE
                                                 ltrim(RTRIM(src_order_origin_code))='GR'
                                             OR  ltrim(RTRIM(src_prepay_code))='F')
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
                    "entdwdb"."fds_cpg"."fact_cpg_sales_header" A
                INNER JOIN
                    (
                        SELECT
                            src_order_number,
                            src_original_ref_order_number
                        FROM
                            "entdwdb"."fds_cpg"."fact_cpg_sales_header"
                        WHERE
                            dim_order_method_id in (select dim_order_method_id from "entdwdb"."fds_cpg"."dim_cpg_order_method"
							where ISNULL(src_channel_id,'0')='R') ) B
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
                SELECT dim_src_kit_item_id,
					a.dim_item_id,
                    src_required_quantity ,
                    AVG(src_component_percent_of_price) AS src_component_percent_of_price
                FROM
                      "entdwdb"."fds_cpg"."dim_cpg_kit_item" A
				INNER JOIN
                    "entdwdb"."fds_cpg"."dim_cpg_item" B
                ON
                   a.dim_src_kit_item_id=b.dim_item_id
                GROUP BY
                    dim_src_kit_item_id,
					a.dim_item_id,
                    src_required_quantity) k
        LEFT OUTER JOIN
            "entdwdb"."fds_cpg"."dim_cpg_item" c
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
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id,dim_src_kit_item_id) Tab_Kit_Regular
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id, src_order_type ,Date,dim_item_id,dim_src_kit_item_id
),  __dbt__CTE__intm_cpg_kit_sales_gratis as (

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
            "entdwdb"."fds_cpg"."fact_cpg_sales_detail"
		LEFT JOIN
            "entdwdb"."fds_cpg"."dim_cpg_order_method"
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
                    "entdwdb"."fds_cpg"."dim_cpg_kit_item" A,
                    "entdwdb"."fds_cpg"."dim_cpg_item" B
                WHERE
                    A.dim_src_kit_item_id=B.dim_item_id)
					AND src_order_number IN
                                 (
                                 SELECT DISTINCT
                                     ltrim(RTRIM(src_order_number)) AS src_order_number
                                 FROM
                                     "entdwdb"."fds_cpg"."fact_cpg_sales_header"
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
                    "entdwdb"."fds_cpg"."dim_cpg_kit_item" A
				INNER JOIN
                    "entdwdb"."fds_cpg"."dim_cpg_item" B
                ON
                    a.dim_src_kit_item_id=b.dim_item_id ) k
        LEFT OUTER JOIN
            "entdwdb"."fds_cpg"."dim_cpg_item" c
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
            "entdwdb"."fds_cpg"."fact_cpg_sales_detail"
		LEFT JOIN
            "entdwdb"."fds_cpg"."dim_cpg_order_method"
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
                    "entdwdb"."fds_cpg"."dim_cpg_kit_item" A,
                    "entdwdb"."fds_cpg"."dim_cpg_item" B
                WHERE
                    A.src_kit_id=B.src_item_id )
        AND src_order_number IN
                                 (
                                 SELECT DISTINCT
                                     ltrim(RTRIM(src_order_number)) AS src_order_number
                                 FROM
                                     "entdwdb"."fds_cpg"."fact_cpg_sales_header"
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
                    "entdwdb"."fds_cpg"."dim_cpg_kit_item" A
				INNER JOIN
                    "entdwdb"."fds_cpg"."dim_cpg_kit_item" B
                ON
                   a.dim_src_kit_item_id=b.dim_item_id ) k
        LEFT OUTER JOIN
            "entdwdb"."fds_cpg"."dim_cpg_item" c
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
                    "entdwdb"."fds_cpg"."fact_cpg_sales_detail"
				LEFT JOIN
					"entdwdb"."fds_cpg"."fact_cpg_sales_detail"
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
                            "entdwdb"."fds_cpg"."fact_cpg_sales_detail" A,
                            "entdwdb"."fds_cpg"."dim_cpg_item" B
                        WHERE
                            A.dim_src_kit_item_id=B.dim_item_id)
							AND src_order_number IN
                                         (
                                         SELECT DISTINCT
                                             ltrim(RTRIM(src_order_number)) AS src_order_number
                                         FROM
                                             "entdwdb"."fds_cpg"."fact_cpg_sales_header"
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
                    "entdwdb"."fds_cpg"."fact_cpg_sales_header" A
                INNER JOIN
                    (
                        SELECT
                            src_order_number,
                            src_original_ref_order_number
                        FROM
                            "entdwdb"."fds_cpg"."fact_cpg_sales_header"
                        WHERE
                            dim_order_method_id in (select dim_order_method_id from "entdwdb"."fds_cpg"."dim_cpg_order_method"
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
                      "entdwdb"."fds_cpg"."dim_cpg_kit_item" A
				INNER JOIN
                    "entdwdb"."fds_cpg"."dim_cpg_kit_item" B
                ON
                  a.dim_src_kit_item_id=b.dim_item_id
                GROUP BY
                    dim_src_kit_item_id,
					a.dim_item_id,
                    src_required_quantity) k
        LEFT OUTER JOIN
            "entdwdb"."fds_cpg"."dim_cpg_item" c
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
),  __dbt__CTE__intm_cpg_kit_sales_free as (

Select dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
  ,dim_order_method_id
  ,case src_order_type when 'C' then 'F' else 'F' end as src_order_type  
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
								,case src_order_type when 'I' then 'F' else 'F' end as src_order_type     
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
            "entdwdb"."fds_cpg"."fact_cpg_sales_detail"
		LEFT JOIN
            "entdwdb"."fds_cpg"."fact_cpg_sales_detail"
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
                    "entdwdb"."fds_cpg"."dim_cpg_kit_item" A,
                    "entdwdb"."fds_cpg"."dim_cpg_item" B
                WHERE
					A.dim_src_kit_item_id=B.dim_item_id)
					AND src_order_number IN
                                 (
                                 SELECT DISTINCT
                                     ltrim(RTRIM(src_order_number)) AS src_order_number
                                 FROM
                                     "entdwdb"."fds_cpg"."fact_cpg_sales_header"
                                 WHERE
                                     ltrim(RTRIM(src_prepay_code))='F'
                                 AND ISNULL(ltrim(RTRIM(src_order_origin_code)),'AA')<>'GR')
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
    (src_component_percent_of_price)       AS src_component_percent_of_price ,
    (src_required_quantity)                AS src_required_quantity
                FROM
                    "entdwdb"."fds_cpg"."dim_cpg_kit_item" A
				INNER JOIN
                    "entdwdb"."fds_cpg"."dim_cpg_kit_item" B
                ON
                    a.dim_src_kit_item_id=b.dim_item_id
) k
        LEFT OUTER JOIN
            "entdwdb"."fds_cpg"."dim_cpg_item" c
        ON
           k.dim_item_id=c.dim_item_id
        GROUP BY
           k.dim_src_kit_item_id,c.dim_item_id) B
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
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id,src_order_type,Date,dim_item_id ,dim_src_kit_item         
	UNION all
/*Retrieving Shipped sales for Kit Items*/    
	SELECT dim_business_unit_id,dim_shop_site_id,src_currency_code_from  
								 ,dim_order_method_id
								 ,case src_order_type when 'I' then 'F' else 'F' end as src_order_type      
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
    B.dim_item_id AS dim_item_id,
    A.dim_item_id AS dim_src_kit_item_id,
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
            "entdwdb"."fds_cpg"."fact_cpg_sales_detail"
		LEFT JOIN
            "entdwdb"."fds_cpg"."fact_cpg_sales_detail"
        ON
            fact_cpg_sales_detail.dim_order_method_id=
            dim_cpg_order_method.dim_order_method_id
        WHERE
            src_order_type='I'
        AND Ship_Date_id IS NOT NULL
        AND ISNULL(src_channel_id,'0') <> 'R'
        AND fact_cpg_sales_detail.dim_item_id IN
                    (
                        SELECT DISTINCT
                            A.dim_src_kit_item_id
                        FROM
                            "entdwdb"."fds_cpg"."dim_cpg_kit_item" A,
                            "entdwdb"."fds_cpg"."dim_cpg_item" B
                        WHERE
							A.dim_src_kit_item_id=B.dim_item_id )
							AND src_order_number IN
                                 (
                                 SELECT DISTINCT
                                     ltrim(RTRIM(src_order_number)) AS src_order_number
                                 FROM
                                     "entdwdb"."fds_cpg"."fact_cpg_sales_header"
                                 WHERE
                                     ltrim(RTRIM(src_prepay_code))='F'
                                 AND ISNULL(ltrim(RTRIM(src_order_origin_code)),'AA')<>'GR')
        AND src_order_Status='IN'
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
    (src_component_percent_of_price)       AS src_component_percent_of_price ,
    (src_required_quantity)                AS src_required_quantity
                FROM
                    "entdwdb"."fds_cpg"."dim_cpg_kit_item" A
				INNER JOIN
                    "entdwdb"."fds_cpg"."dim_cpg_item" B
                ON
                  a.dim_src_kit_item_id=b.dim_item_id
) k
        LEFT OUTER JOIN
            "entdwdb"."fds_cpg"."dim_cpg_item" c
        ON
             k.dim_item_id=c.dim_item_id
        GROUP BY
             k.dim_src_kit_item_id,c.dim_item_id) B
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
								 ,case src_order_type when 'C' then 'F' else 'F' end as src_order_type       
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
        THEN 'F'
        ELSE 'F'
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
                THEN 'F'
                ELSE 'F'
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
                        THEN 'F'
                        ELSE 'F'
                    END AS src_order_type ,
                    fact_cpg_sales_detail.dim_item_id,
                    src_order_number,
                    fact_cpg_sales_detail.src_unit_cost,
                    fact_cpg_sales_detail.src_selling_price,
                    fact_cpg_sales_detail.src_current_retail_price,
                    src_units_ordered,
                    order_date_id
                FROM
                    "entdwdb"."fds_cpg"."fact_cpg_sales_detail"
				LEFT JOIN
					"entdwdb"."fds_cpg"."dim_cpg_order_method"
				ON
            fact_cpg_sales_detail.dim_order_method_id=
            dim_cpg_order_method.dim_order_method_id
                WHERE
                ISNULL(src_channel_id,'0')='R'
				AND src_Order_Status='IN'
                AND fact_cpg_sales_detail.dim_item_id IN
                    (
                        SELECT DISTINCT
                                A.dim_src_kit_item_id
                        FROM
                            "entdwdb"."fds_cpg"."dim_cpg_kit_item" A,
                            "entdwdb"."fds_cpg"."dim_cpg_item" B
                        WHERE
                             A.dim_src_kit_item_id=B.dim_item_id)
							AND src_order_number IN
                                         (
                                         SELECT DISTINCT
                                             ltrim(RTRIM(src_order_number)) AS src_order_number
                                         FROM
                                             "entdwdb"."fds_cpg"."fact_cpg_sales_header"
                                         WHERE
                                             ltrim(RTRIM(src_prepay_code))='F'
                                         AND ISNULL(ltrim(RTRIM(src_order_origin_code)),'AA')<>'GR'
                                         )
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
                    order_date_id  ) AS A
        LEFT OUTER JOIN
            (
                SELECT
                    B.src_order_number    AS src_order_number,
                    A.dim_order_method_id AS dim_order_method_id
                FROM
                    "entdwdb"."fds_cpg"."fact_cpg_sales_header" A
                INNER JOIN
                    (
                        SELECT
                            src_order_number,
                            src_original_ref_order_number
                        FROM
                            "entdwdb"."fds_cpg"."fact_cpg_sales_header"
                        WHERE
                            dim_order_method_id in (select dim_order_method_id from "entdwdb"."fds_cpg"."dim_cpg_item"
							where ISNULL(src_channel_id,'0')='R') ) B
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
                SELECT  dim_src_kit_item_id,
					a.dim_item_id,
                    src_required_quantity ,
                    AVG(src_component_percent_of_price) AS src_component_percent_of_price
                FROM
                      "entdwdb"."fds_cpg"."dim_cpg_kit_item" A
				INNER JOIN
                    "entdwdb"."fds_cpg"."dim_cpg_item" B
                ON
                   a.dim_src_kit_item_id=b.dim_item_id
                GROUP BY
                    dim_src_kit_item_id,a.dim_item_id,src_required_quantity
) k
        LEFT OUTER JOIN
            "entdwdb"."fds_cpg"."dim_cpg_item" c
        ON
          k.dim_item_id=c.dim_item_id
        GROUP BY
                 k.dim_src_kit_item_id,c.dim_item_id) B
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
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from  ,dim_order_method_id, src_order_type ,Date,dim_item_id,dim_src_kit_item_id) Tab_Kit_Free_Orders
group by dim_business_unit_id,dim_shop_site_id,src_currency_code_from  ,dim_order_method_id, src_order_type ,Date,dim_item_id,dim_src_kit_item_id
),  __dbt__CTE__intm_cpg_kit_sales as (

select dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id,cast(src_order_type as varchar(1)) as src_order_type,date_key,dim_item_id,dim_src_kit_item_id,src_unit_cost,src_current_retail_price,
src_units_ordered,src_units_shipped,units_returned,net_units_sold,demand_cogs$ demand_cogs_$,shipped_cogs$
shipped_cogs_$,returned_cogs$ returned_cogs_$,net_cogs$ net_cogs_$,demand_retail$ demand_retail_$,shipped_retail$ shipped_retail_$,
net_retail$ net_retail_$,demand_sales$ demand_sales_$,shipped_sales$ shipped_sales_$,return$ returns_$,
net$ net_sales_$,demand_selling_margin$ demand_selling_margin_$,shipped_selling_margin$ shipped_selling_margin_$,
net_selling_margin$ net_selling_margin_$
from
(
	select * from __dbt__CTE__intm_cpg_kit_sales_regular
	union all
	select * from __dbt__CTE__intm_cpg_kit_sales_gratis
	union all
	select * from __dbt__CTE__intm_cpg_kit_sales_free
)
)select dim_business_unit_id,
dim_order_method_id,
date_key,
src_order_type,
dim_src_kit_item_id as dim_kit_item_id,
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
0 as other_amount,
0 as kit_units_sold,
(src_unit_cost/conversion_rate_to_local) as unit_cost_local,
(src_current_retail_price/conversion_rate_to_local) as current_retail_price_local,
(demand_cogs_$/conversion_rate_to_local) as demand_cogs_local,
(shipped_cogs_$/conversion_rate_to_local) as shipped_cogs_local,
(returned_cogs_$/conversion_rate_to_local) as returned_cogs_local,
(net_cogs_$/conversion_rate_to_local) as net_cogs_local,
(demand_retail_$/conversion_rate_to_local) as demand_retail_local,
(shipped_retail_$/conversion_rate_to_local) as shipped_retail_local,
(net_retail_$/conversion_rate_to_local) as net_retail_local,
(demand_sales_$/conversion_rate_to_local) as demand_sales_local,
(shipped_sales_$/conversion_rate_to_local) as shipped_sales_local,
(returns_$/conversion_rate_to_local) as returns_local,
(net_sales_$/conversion_rate_to_local) as net_sales_local,
(demand_selling_margin_$/conversion_rate_to_local) as demand_selling_margin_local,
(shipped_selling_margin_$/conversion_rate_to_local) as shipped_selling_margin_local,
(net_selling_margin_$/conversion_rate_to_local) as net_selling_margin_local,
0 as other_amount_local,
src_currency_code_from,
dim_shop_site_id,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_4B' as etl_batch_id,
'bi_dbt_user_prd' as etl_insert_user_id, current_timestamp as etl_insert_rec_dttm, 
null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from 
(
select src.*, 
nullif((case when src.src_currency_code_from='USD' then 1 else cc.currency_conversion_rate_spot_rate end),0) as conversion_rate_to_local
from __dbt__CTE__intm_cpg_kit_sales src
left outer join "entdwdb"."dt_stage"."prestg_cpg_currency_exchange_rate" cc
on cast(cast(src.date_key as varchar(20)) as date) =cc.as_on_date 
and src.src_currency_code_from=cc.currency_code_from 
and cc.currency_code_to='USD'
)