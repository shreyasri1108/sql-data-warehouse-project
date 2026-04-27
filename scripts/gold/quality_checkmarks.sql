/*
Purpose :- The below shows quality checkmarks for gold layer to check data integriy
Sarrogate key working , when data integrated for Gold layer
Also checks vaildations in data model between he dimesnions and fact table
*/

-- VALIDATING FACT TABLE INTEGRATON WITH DIMENSION TABLE

-- FACT check - check  if all dimension tables can successfully connect to fact table
SELECT
*
FROM Gold.fact_sales as f
left join Gold.dim_customers as c
on f.customer_key = c.customer_key
where c.customer_key is null -- just checking
