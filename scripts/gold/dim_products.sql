/*
Purpose :- The below is the gold layer products dimension working on STAR schema 
The below table has all details regarding products their sales and other related details

The below will be available in VIEW format , all the details comes from silver layer having clear and enriched amout of data
*/	

-- CREATING A VIEW FOR BELOW

IF OBJECT_ID ( 'Gold.dim_products' , 'V') IS NOT NULL
	DROP VIEW Gold.dim_products;
GO
CREATE VIEW Gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY prd_key , prd_start_dt) AS product_key , -- defining a sarogate key
	pd.prd_id as product_id,
	pd.prd_key as product_number,
	pd.prd_nm as product_name,
	pd.cat_id as category_id,
	cat.CAT as category,
	cat.SUNCAT as subcategory,
	cat.MAINTENANCE,
	pd.prd_cost as cost,
	pd.prd_line as product_line,
	pd.prd_start_dt as start_date
FROM Silver.crm_prd_info AS pd
LEFT JOIN Silver.erp_px_cat_g1v2 as cat
on pd.cat_id = cat.ID
WHERE prd_end_dt IS NULL -- filtering out historical data for same product id
