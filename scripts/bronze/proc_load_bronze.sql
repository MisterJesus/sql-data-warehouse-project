/*
========================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
========================================================================
Script Purpose:
            This stored procedure 
            - Truncates the bronze tables before loading data.
            - Uses the BULK INSERT command to load data from csv Files to bronze tables.

Parameters:
            None.
            This stored procedure does not accept any parameters or return any values.

Usage Example:
            EXEC bronze.load_bronze;
*/

-- Run after executing the CREATE OR ALTER PROCEDURE --

EXEC bronze.load_bronze;

-- Data Ingestion Script --

CREATE OR ALTER PROCEDURE bronze.load_bronze AS	-- Making a script since this is a query you will be running everyday to grab updated data --
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @script_start_time DATETIME, @script_end_time DATETIME;
	BEGIN TRY
		SET @script_start_time = GETDATE();		-- Start of the script's runtime --
		PRINT '==========================';
		PRINT '-- Loading Bronze Layer --';		-- Aesthetics for the output --
		PRINT '==========================';

		PRINT '==========================';
		PRINT '--  Loading CRM tables  --';		-- Aesthetics --
		PRINT '==========================';

		SET @start_time = GETDATE();			-- Start of a table's runtime --
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;	-- Deletes all values in the table, allows you to BULK INSERT without worries of duplicates --

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info		-- Instead of inserting data manually, we will do a BULK INSERT from a .txt or .csv file --
		FROM 'C:\Users\Miguel\Desktop\SQL Course Materials\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH 
		(
			FIRSTROW = 2,			-- Tells SQL to skip the first row of the .csv file since that is the header --
			FIELDTERMINATOR = ',',	-- Tells SQL what the delimiter is, in this case, the ',' (comma) --
			TABLOCK					-- As SQL loads the table, it also locks it --
		);
		SET @end_time = GETDATE();				-- End of a table's runtime. Repeated for each table. --
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '==========================';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Miguel\Desktop\SQL Course Materials\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK					
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '==========================';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into: crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Miguel\Desktop\SQL Course Materials\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK					
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '==========================';

		PRINT '==========================';
		PRINT '--  Loading ERP tables  --';		-- Aesthetics --
		PRINT '==========================';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Miguel\Desktop\SQL Course Materials\sql-data-warehouse-project-main\datasets\source_erp\cust_az12.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK					
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '==========================';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Miguel\Desktop\SQL Course Materials\sql-data-warehouse-project-main\datasets\source_erp\loc_a101.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK					
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '==========================';
	
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Miguel\Desktop\SQL Course Materials\sql-data-warehouse-project-main\datasets\source_erp\px_cat_g1v2.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK					
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '==========================';

		SET @script_end_time = GETDATE();				-- End of the script's runtime --
		PRINT '==========================';
		PRINT 'Loading Bronze Layer is Completed';
		PRINT 'Total Load Duration: ' + CAST(DATEDIFF(SECOND, @script_start_time, @script_end_time) AS NVARCHAR) + ' seconds';
		PRINT '==========================';
	END TRY

	BEGIN CATCH
		PRINT '==========================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Message: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================';
	END CATCH

END;			-- Procedures are found in Server > Databases > DataWarehouse > Programmability > Stored Procedures > bronze.load_bronze
