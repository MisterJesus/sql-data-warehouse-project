-- Chapter 12.1.2: Project 1 - Data Warehouse (Bronze Layer - Inserting Data) --

/*
===========================================================================
DDL Script: Create Bronze Tables
===========================================================================
Script Purpose: 
            This script creates tables in the 'bronze' schema, 
            dropping existing tables if they already exist.
            Run this script to re-define DDL structure of 'bronze' tables
===========================================================================
*/

-- This is an execution for the script made from the query below --
EXEC bronze.load_bronze;		-- If code below has been executed then execute this only for succeeding executions. If not, hightlight entire code below then execute once.  --

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


-- Just to check the count of the rows if it coincides with the number of rows of the cvs in Excel (minus the header) --
SELECT COUNT(*) FROM bronze.crm_cust_info;
SELECT COUNT(*) FROM bronze.crm_prd_info;
SELECT COUNT(*) FROM bronze.crm_sales_details;
SELECT COUNT(*) FROM bronze.erp_cust_az12;
SELECT COUNT(*) FROM bronze.erp_loc_a101;
SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;

/* [Note] How this script began: 
- First we did a BULK INSERT for all the tables we created. 
- Then make a Truncate query above for each of those BULK INSERTs
- We made a create or alter PROCEDURE, so we don't have to execute all of that code
- Add a Try and Catch in the code in case of loading errors
- Add a way to track ETL duration for each insertion via DECLARE and SET for each table from truncate to bulk insert
- Also add in the DECLARE a start and endtimes for the entire script to know the runtime of the whole script
- ETL process done
*/
