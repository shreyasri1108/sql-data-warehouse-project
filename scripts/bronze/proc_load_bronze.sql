/*
Purpose :- Loading values fom .csv files to a SQL server using TRUNCATE AND INSERT method
Note :- Here TRUNCATE first clears all data in a table and then freshly inserts new data 

TRY - CATCH statements are used to find glitch during data loading

ALSO below script calculates the time to load data 
*/
EXEC Bronze.load_bronze;
-- CREATING STORED PROEDURE

CREATE OR ALTER PROCEDURE Bronze.load_bronze AS 
BEGIN
    DECLARE @start_time DATETIME , @end_time DATETIME , @batch_start_time DATETIME , @batch_end_time DATETIME

		
	BEGIN TRY
		SET @batch_start_time = GETDATE()
		PRINT('===================================================================');
		PRINT('LOADING BRONZE LAYER ...');
		PRINT('===================================================================');

		PRINT('-------------------------------------------------------------------');
		PRINT('LOADING CRM DATA');
		PRINT('-------------------------------------------------------------------');

		PRINT('TRUNCATING DATA ');
		TRUNCATE TABLE Bronze.crm_cust_info; -- first empty the table
		SET @start_time = GETDATE()
		PRINT('Inserting data into : Bronze.crm_cust_info');
		BULK INSERT Bronze.crm_cust_info
		FROM 'C:\Users\Ayush\OneDrive\Desktop\IMP-DOCS\Learning\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.CSV'
		WITH (
			FIRSTROW = 2 ,  -- this means that table's firstrow starts from 2nd row of csv , as 1st row of csv is heading
			FIELDTERMINATOR = ',',  -- this is separator used b/w values
			TABLOCK -- for optimizing table
		);
		SET @end_time = GETDATE()
		PRINT('TIME TAKEN '+ CAST(DATEDIFF(SECOND ,@start_time , @end_time ) AS NVARCHAR)+ ' seconds')

		PRINT('TRUNCATING DATA ');
		TRUNCATE TABLE Bronze.crm_prd_info; 
		PRINT('Inserting data into : Bronze.crm_prd_info');
		SET @start_time = GETDATE()
		BULK INSERT Bronze.crm_prd_info
		FROM 'C:\Users\Ayush\OneDrive\Desktop\IMP-DOCS\Learning\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.CSV'
		WITH (
			FIRSTROW = 2 ,  
			FIELDTERMINATOR = ',',  
			TABLOCK 
		);
		SET @end_time = GETDATE()
		PRINT('TIME TAKEN '+ CAST(DATEDIFF(SECOND ,@start_time , @end_time ) AS NVARCHAR)+ ' seconds')

		PRINT('TRUNCATING DATA ');
		TRUNCATE TABLE Bronze.crm_sales_details; 
		PRINT('Inserting data into : Bronze.crm_sales_details');
		SET @start_time = GETDATE()
		BULK INSERT Bronze.crm_sales_details
		FROM 'C:\Users\Ayush\OneDrive\Desktop\IMP-DOCS\Learning\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.CSV'
		WITH (
			FIRSTROW = 2 ,  
			FIELDTERMINATOR = ',',  
			TABLOCK 
		);
		SET @end_time = GETDATE()
		PRINT('TIME TAKEN '+ CAST(DATEDIFF(SECOND ,@start_time , @end_time ) AS NVARCHAR)+ ' seconds')

		PRINT('-------------------------------------------------------------------');
		PRINT('LOADING ERP DATA');
		PRINT('-------------------------------------------------------------------');

		PRINT('TRUNCATING DATA ');
		TRUNCATE TABLE Bronze.erp_px_cat_g1v2; 
		PRINT('Inserting data into : Bronze.erp_px_cat_g1v2');
		SET @start_time = GETDATE()
		BULK INSERT Bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Ayush\OneDrive\Desktop\IMP-DOCS\Learning\SQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.CSV'
		WITH (
			FIRSTROW = 2 ,  
			FIELDTERMINATOR = ',',  
			TABLOCK 
		);
		SET @end_time = GETDATE()
		PRINT('TIME TAKEN '+ CAST(DATEDIFF(SECOND ,@start_time , @end_time ) AS NVARCHAR)+ ' seconds')

		PRINT('TRUNCATING DATA ');
		TRUNCATE TABLE Bronze.erp_cust_az12; 
		PRINT('Inserting data into : Bronze.erp_cust_az12');
		SET @start_time = GETDATE()
		BULK INSERT Bronze.erp_cust_az12
		FROM 'C:\Users\Ayush\OneDrive\Desktop\IMP-DOCS\Learning\SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.CSV'
		WITH (
			FIRSTROW = 2 ,  
			FIELDTERMINATOR = ',',  
			TABLOCK 
		);
		SET @end_time = GETDATE()
		PRINT('TIME TAKEN '+ CAST(DATEDIFF(SECOND ,@start_time , @end_time ) AS NVARCHAR)+ ' seconds')

		PRINT('TRUNCATING DATA ');
		TRUNCATE TABLE Bronze.erp_loc_a101; 
		PRINT('Inserting data into : Bronze.erp_loc_a101');
		SET @start_time = GETDATE()
		BULK INSERT Bronze.erp_loc_a101
		FROM 'C:\Users\Ayush\OneDrive\Desktop\IMP-DOCS\Learning\SQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.CSV'
		WITH (
			FIRSTROW = 2 ,  
			FIELDTERMINATOR = ',',  
			TABLOCK 
		);
		SET @end_time = GETDATE()
		PRINT('TIME TAKEN '+ CAST(DATEDIFF(SECOND ,@start_time , @end_time ) AS NVARCHAR)+ ' seconds')

		SET @batch_end_time = GETDATE()
		PRINT ('----------------')
	    PRINT ('TOTAL BRONZE LAYER LOAD TIME ' + cast(DATEDIFF(SECOND , @batch_start_time , @batch_end_time) as varchar) + ' seconds')

		END TRY

		BEGIN CATCH
		    PRINT('===================================================================');
			PRINT('ERROR OCCURED IN BRONZE LAYER ...');
			PRINT('===================================================================');
		    PRINT('ERROR MESSAGE IS ' + ERROR_MESSAGE())
			PRINT('ERROR NUMBER IS ' + CAST(ERROR_NUMBER() AS NVARCHAR))
			PRINT('ERROR STATE IS ' + CAST(ERROR_STATE() AS NVARCHAR))
		END CATCH;
		
END;

