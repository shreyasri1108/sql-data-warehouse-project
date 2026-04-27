/*
Purpose :- The below is the gold layer customer dimension working on STAR schema 
The below table has all details regarding customers their locations and birth details

The below will be available in VIEW format , all the details comes from silver layer having clear and enriched amout of data
*/	
-- BUILDING GOLD LAYER
-- creating a view for following

IF OBJECT_ID ( 'Gold.dim_customers' , 'V') IS NOT NULL
	DROP VIEW Gold.dim_customers;
GO
CREATE VIEW Gold.dim_customers AS
SELECT 
-- creating a sarrogate key
ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
ci.cst_id AS customer_id,
ci.cst_key as customer_name,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.CNTRY as country,
ci.cst_marital_status as marital_status,
case WHEN ci.cst_gndr != 'n/a' then ci.cst_gndr
	else ISNULL(ca.GEN, 'n/a')
end gender,
ca.BDATE as birthdate ,
ci.cst_create_date as create_date
FROM Silver.crm_cust_info AS ci
LEFT JOIN Silver.erp_cust_az12 as ca
on ci.cst_key = ca.CID
LEFT JOIN Silver.erp_loc_a101 as la
on ci.cst_key = la.CID
