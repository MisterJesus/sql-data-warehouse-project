-- Chapter 12.1.3: Project 1 - Data Warehouse (Silver Layer - Adding Metadata Columns) --

/*
===========================================================================
DDL Script: Create Silver Tables
===========================================================================
Script Purpose:
            This script creates tables in the 'silver' schema,
            dropping existing tables if they already exist. 
			Run this script to re-define the DDL structure of 'bronze' Table:
===========================================================================
*/

/* Yes. The Silver Layer is very similar to the Bronze Layer - Creating Tables file. 
We just replaced all bronze. -> silver. texts (You can use a Notepad or Notepad++ so you can replace it easily in bulk)
Then we are adding metadata in this step. Just one: dwh_create_date DATETIME2 DEFAULT GETDATE() 
Executing this should give us a total of 12 tables now so far */

IF OBJECT_ID ('silver.crm_cust_info', 'U') IS NOT NULL	-- Checks if table exists. 'U' means User-Defined Table --
	DROP TABLE silver.crm_cust_info;					-- This code helps recreate table from scratch if it exists. --
GO														-- Be careful as it deletes existing data --
CREATE TABLE silver.crm_cust_info 
(
	cst_id INT,
	cst_key	NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE,
	-- NEW in Silver Layer --
	dwh_create_date DATETIME2 DEFAULT GETDATE()			-- Column stores date/time, default value uses the current timestamp. --
);
GO
IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
GO
CREATE TABLE silver.crm_prd_info
(
	prd_id INT,
	cat_id NVARCHAR(50),							-- New Column --
	prd_key	NVARCHAR(50),							-- Removed old prd_key for this new prd_key --
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,								-- Changed from DATETIME to DATE --
	prd_end_dt DATE,								-- Changed from DATETIME to DATE --
	-- NEW in Silver Layer --
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL	
	DROP TABLE silver.crm_sales_details;
GO
CREATE TABLE silver.crm_sales_details
(
	sls_ord_num	NVARCHAR(50),						
	sls_prd_key	NVARCHAR(50),						
	sls_cust_id	INT,								
	sls_order_dt DATE,								-- Changed from INT to DATE --
	sls_ship_dt	DATE,								-- Changed from INT to DATE --
	sls_due_dt DATE,								-- Changed from INT to DATE --
	sls_sales INT,
	sls_quantity INT,	
	sls_price INT,
	-- NEW in Silver Layer --
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
IF OBJECT_ID ('silver.erp_cust_az12', 'U') IS NOT NULL	
	DROP TABLE silver.erp_cust_az12;
GO
CREATE TABLE silver.erp_cust_az12
(
	CID NVARCHAR(50),
	BDATE DATE,
	GEN NVARCHAR(10),
	-- NEW in Silver Layer --
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
IF OBJECT_ID ('silver.erp_loc_a101', 'U') IS NOT NULL	
	DROP TABLE silver.erp_loc_a101;
GO
CREATE TABLE silver.erp_loc_a101
(
	CID NVARCHAR(50),
	CNTRY NVARCHAR(50),
	-- NEW in Silver Layer --
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
IF OBJECT_ID ('silver.erp_px_cat_g1v2', 'U') IS NOT NULL	
	DROP TABLE silver.erp_px_cat_g1v2;
GO
CREATE TABLE silver.erp_px_cat_g1v2
(
	ID NVARCHAR(50),
	CAT NVARCHAR(50),
	SUBCAT NVARCHAR(50),
	MAINTENANCE NVARCHAR(10),
	-- NEW in Silver Layer --
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
