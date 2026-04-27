/*
Purpose :- The below is the gold layer FACT table of sales  working on STAR schema 
The below table has orders details along with 2 foregin keys joining both dimension table with
FACT table

The below will be available in VIEW format , all the details comes from silver layer having clear and enriched amout of data

USAGE :-
        This helps in data analysis and reporting
*/	

-- creating view for star schema
-- REMEMBER - OBJECT_ID  is used for 'U' -> USER TABLE , 'V'-> VIEW , 'P'-> PROCEDURE
IF OBJECT_ID ( 'Gold.fact_sales' , 'V') IS NOT NULL
	DROP VIEW Gold.fact_sales;
GO
CREATE VIEW Gold.fact_sales AS
SELECT
sd.sls_ord_num as order_name,
dp.product_key, -- sarogate key from dimension products
dc.customer_key, -- sarogate key from dimension customers
sd.sls_order_dt as order_date,
sd.sls_ship_dt as ship_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales,
sd.sls_quantity as quantity,
sd.sls_price as price
FROM Silver.crm_sales_details AS sd
LEFT JOIN GOLD.dim_products AS dp
ON sd.sls_prd_key = dp.product_number
LEFT JOIN GOLD.dim_customers AS dc
ON sd.sls_cust_id = dc.customer_id


