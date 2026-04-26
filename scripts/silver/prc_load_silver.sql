/*
======================================================================
        Load BRONZE DATA -> SILVER LAYER
======================================================================
Purpose :- The below script performs ETL (Extract , Transform , Load) tranforming and cleaning data from Bronze -> Silver Layer

Action performed :- a- Truncates Data if alreay exists then
                    b- Inserts clean and transformed data
*/

EXEC Silver.load_silver

CREATE OR ALTER PROCEDURE Silver.load_silver AS
 BEGIN
	 DECLARE @start_time DATETIME , @end_time DATETIME , @total_start_time DATETIME , @total_end_time DATETIME
	 SET @total_start_time = GETDATE()
	 BEGIN TRY 
		PRINT('===================================================================');
		PRINT('LOADING SILVER LAYER ...');
		PRINT('===================================================================');

		PRINT('-------------------------------------------------------------------');
		PRINT('LOADING TRANSFORMED CRM DATA ');
		PRINT('-------------------------------------------------------------------');

		SET @start_time = GETDATE()

		IF OBJECT_ID('Silver.crm_cust_info' , 'U') IS NOT NULL
			PRINT('TRUNCATING DATA ');
			TRUNCATE TABLE Silver.crm_cust_info
			PRINT('Loading data into ' + 'Silver.crm_cust_info table')

			INSERT INTO Silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date )

			SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) cst_firstname,
			TRIM(cst_lastname) cst_lastname,
			CASE
			   WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
			   WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
			   ELSE 'n/a'
			END cst_marital_status,
			CASE
			   WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
			   WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
			   ELSE 'n/a'
			END cst_gndr,
			cst_create_date
			FROM(
				SELECT 
				*,
				ROW_NUMBER() OVER(partition by cst_id order by cst_create_date desc ) as flag_last_date
				FROM Bronze.crm_cust_info) t
			WHERE flag_last_date = 1 and cst_id IS NOT NULL
		 SET @end_time = GETDATE()

		 PRINT('TIME TAKEN '+ CAST(DATEDIFF(SECOND ,@start_time , @end_time ) AS NVARCHAR)+ ' seconds')

		 SET @start_time = GETDATE()
		 IF OBJECT_ID('Silver.crm_prd_info' , 'U') IS NOT NULL
			PRINT('TRUNCATING DATA ');
			TRUNCATE TABLE Silver.crm_prd_info
			PRINT('Loading data into ' + 'Silver.crm_prd_info table')

			INSERT INTO Silver.crm_prd_info (
			prd_id  ,
			cat_id ,
			prd_key ,
			prd_nm ,
			prd_cost,
			prd_line ,
			prd_start_dt ,
			prd_end_dt 
			)
			SELECT 
			prd_id,
			REPLACE(SUBSTRING(UPPER(prd_key) , 1 , 5) , '-' , '_') AS cat_id,
			SUBSTRING(UPPER(prd_key) ,7 , LEN(prd_key )) AS prd_key,
			prd_nm,
			ISNULL(prd_cost , 0),
			CASE UPPER(trim(prd_line))
				WHEN  'R' then 'Road'
				WHEN  'M' then 'Mountains'
				WHEN  'S' then 'Other Sales'
				WHEN  'T' then 'Touring'
				ELSE 'n/a'
			END prd_cost,
			prd_start_dt,
			DATEADD (DAY , -1 , LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt ASC)) prd_end_dt
			FROM Bronze.crm_prd_info
		 SET @end_time = GETDATE()

		 PRINT('TIME TAKEN '+ CAST(DATEDIFF(SECOND ,@start_time , @end_time ) AS NVARCHAR)+ ' seconds')

		 SET @start_time = GETDATE()
		 IF OBJECT_ID('Silver.crm_sales_details' , 'U') IS NOT NULL
			PRINT('TRUNCATING DATA ');
			TRUNCATE TABLE Silver.crm_sales_details
			PRINT('Loading data into ' + 'Silver.crm_sales_details')
			INSERT INTO Silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
			)
			SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN LEN(sls_order_dt)!=8 OR sls_order_dt < 1 THEN NULL
				 ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE)
			END sls_order_dt,
			CASE WHEN LEN(sls_ship_dt)!=8 OR sls_ship_dt < 1 THEN NULL
				 ELSE CAST(CAST(sls_ship_dt AS VARCHAR)AS DATE)
			END sls_ship_dt,
			CASE WHEN LEN(sls_due_dt)!=8 OR sls_due_dt < 1 THEN NULL
				 ELSE CAST(CAST(sls_due_dt AS VARCHAR)AS DATE)
			END sls_due_dt,
			CASE WHEN sls_sales IS NULL OR sls_sales != ABS(sls_quantity) * ABS(sls_price) THEN ABS(sls_quantity) * ABS(sls_price)
				 ELSE ABS( sls_sales) 
			END sls_sales,
			ABS(sls_quantity) AS sls_quantity,
			CASE WHEN sls_price IS NULL OR  sls_price<=0 THEN ABS(sls_sales)/NULLIF(sls_quantity,0)
				 ELSE ABS(sls_price) 
			END sls_price
			FROM Bronze.crm_sales_details
		 SET @end_time = GETDATE()

		 PRINT('TIME TAKEN '+ CAST(DATEDIFF(SECOND ,@start_time , @end_time ) AS NVARCHAR)+ ' seconds')




		PRINT('-------------------------------------------------------------------');
		PRINT('LOADING TRANSFORMED ERP  DATA');
		PRINT('-------------------------------------------------------------------');

		SET @start_time = GETDATE()
		IF OBJECT_ID('Silver.erp_cust_az12' , 'U') IS NOT NULL
			PRINT('TRUNCATING DATA ');
			TRUNCATE TABLE Silver.erp_cust_az12
			PRINT('Loading data into ' + 'Silver.erp_cust_az12 table')
			INSERT INTO Silver.erp_cust_az12
			(
			CID  ,
			BDATE ,
			GEN 
			)
			SELECT 
			CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID , 4 , LEN(CID))
				 ELSE CID
			END CID,
			CASE WHEN  BDATE > GETDATE() THEN NULL
				 ELSE BDATE 
			END BDATE ,
			CASE WHEN TRIM(GEN) IN ('F' , 'Female') then 'Female'
				 WHEN TRIM(GEN) IN ('M' , 'Male') then 'Male'
				 ELSE NULL
			END GEN
			FROM Bronze.erp_cust_az12
		 SET @end_time = GETDATE()

		 PRINT('TIME TAKEN '+ CAST(DATEDIFF(SECOND ,@start_time , @end_time ) AS NVARCHAR)+ ' seconds')

		SET @start_time = GETDATE()
		IF OBJECT_ID('SILVER.erp_loc_a101' , 'U') IS NOT NULL
			PRINT('TRUNCATING DATA ');
			TRUNCATE TABLE SILVER.erp_loc_a101
			PRINT('Loading data into ' + 'SILVER.erp_loc_a101 table')
			INSERT INTO SILVER.erp_loc_a101
			(
			CID,
			CNTRY
			)
			SELECT
			REPLACE(trim(CID) , '-' , ''),
			CASE WHEN TRIM(CNTRY) IN ('DE' , 'Germany') THEN 'Germany'
				 WHEN TRIM(CNTRY) IN ('USA' , 'United States') THEN 'United States'
				 WHEN CNTRY IS NULL OR CNTRY='' THEN 'N/A'
				 ELSE CNTRY
			END CNTRY
			FROM Bronze.erp_loc_a101
		 SET @end_time = GETDATE()

		 PRINT('TIME TAKEN '+ CAST(DATEDIFF(SECOND ,@start_time , @end_time ) AS NVARCHAR)+ ' seconds')

		SET @start_time = GETDATE()
		IF OBJECT_ID('Silver.erp_px_cat_g1v2' , 'U') IS NOT NULL
			PRINT('TRUNCATING DATA ');
			TRUNCATE TABLE Silver.erp_px_cat_g1v2
			PRINT('Loading data into ' + 'Silver.erp_px_cat_g1v2 table')
			INSERT INTO Silver.erp_px_cat_g1v2
			(
			ID ,
			CAT  ,
			SUNCAT,
			MAINTENANCE 
			)
			SELECT 
			TRIM(ID) ,
			TRIM(CAT)  ,
			TRIM(SUNCAT),
			TRIM(MAINTENANCE)
			FROM Bronze.erp_px_cat_g1v2
		 SET @end_time = GETDATE()
		 PRINT('TIME TAKEN '+ CAST(DATEDIFF(SECOND ,@start_time , @end_time ) AS NVARCHAR)+ ' seconds')

		END TRY

		BEGIN CATCH
			PRINT('=======================================================')
			PRINT('ERROR HAS OCCURED')
			PRINT('=======================================================')
			PRINT('ERROR MESSAGE IS'+ ERROR_MESSAGE())
			PRINT('ERROR MESSAGE IS ' + ERROR_MESSAGE())
			PRINT('ERROR NUMBER IS ' + CAST(ERROR_NUMBER() AS NVARCHAR))
			PRINT('ERROR STATE IS ' + CAST(ERROR_STATE() AS NVARCHAR))
		  
		  
		END CATCH
		SET @total_end_time = GETDATE()
		PRINT('TOTAL TIME TAKEN '+ CAST(DATEDIFF(SECOND ,@total_start_time , @total_end_time ) AS NVARCHAR)+ ' seconds')
 END
