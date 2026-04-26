/*
Purpose :- To check DATA Stnadardization, Data consistenancy and accuracy across 'silver' schema .
          -> check for null or duplicate value
          -> unwantted spaces
          -> Data standadization and consistenancy
          -> Invalid data ranges or values

Example - below script is a small example
*/

-- CHECK IF ID IS UNQUE AND NOT NULL
SELECT * FROM Bronze.erp_px_cat_g1v2

SELECT * FROM SILVER.crm_cust_info

SELECT 
COUNT(*) 
FROM Bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- check if column having leading and trailing spaces  for 'prd_id' , 'prd_nm'

SELECT 
prd_key
FROM Bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm) or prd_nm is null
-- TO CHECK ABOVE COLUMN'S VA;UE IS EXIST IN SOME ANOTHER TABLE AS WELL
SELECT 
SUBSTRING(UPPER(prd_key) ,7 , LEN(prd_key ))
FROM Bronze.crm_prd_info
WHERE SUBSTRING(UPPER(prd_key) ,7 , LEN(prd_key ))  not IN (
															SELECT id FROM Bronze.erp_px_cat_g1v2 where id like 'AC%')


-- check for 'prd_cost' having negative or null value
SELECT 
prd_cost
FROM Bronze.crm_prd_info
where prd_cost < 1 or prd_cost is null

-- select distinct value from 'prd_line'
SELECT DISTINCT prd_line
from Bronze.crm_prd_info

-- CHECK FOR DATE , where start date is not more than end date
SELECT
prd_key ,
-- using below since end date < start_date , also or same product end date of current id must be 1 less than start date of next id
prd_start_dt,
DATEADD (DAY , -1 , LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt ASC)) prd_end_dt
from Bronze.crm_prd_info
