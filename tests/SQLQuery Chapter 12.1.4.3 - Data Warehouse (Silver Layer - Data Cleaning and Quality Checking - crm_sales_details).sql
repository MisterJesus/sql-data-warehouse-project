-- Chapter 12.1.4.3 - Data Warehouse (Silver Layer - Data Cleaning and Quality Checking - crm_sales_details) --
-- However we will use the bronze.table for cleaning first before inserting it to its silver.table variant --

-- First we study the table to see what kind of cleaning and quality checking we can do --
SELECT
	sls_ord_num, 
	sls_prd_key, 
	sls_cust_id, 
	sls_order_dt, 
	sls_ship_dt, 
	sls_due_dt, 
	sls_sales, 
	sls_quantity, 
	sls_price
FROM bronze.crm_sales_details

-- Starting with sls_ord_num, let's see if there are any unwanted spaces  --
SELECT
	sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)			-- Result: None. That is good

/* 
Next is the sls_prd_key and the sls_cust_id
As we learned when making the integration model, we want to connect the sls_prd_key with the crm_prd_info's prd_key
And the sls_cust_id with the crm_cust_info's cust_id
So we need to check if everything is working properly by test-connecting them
*/
SELECT
	sls_prd_key
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)	-- Since we finished cleaning crm_prd_info, we can use its silver variant --

SELECT
	sls_cust_id
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)	-- Same for the cust_info table --

-- Both results to empty. That is good --

-- Next are sls_order_dt, sls_ship_dt, and sls_due_dt --
SELECT
	sls_order_dt, 
	sls_ship_dt, 
	sls_due_dt
FROM bronze.crm_sales_details			-- As we can see, they're not really 'Dates' they are in the form of INT. So let's change that --

-- First let us check for invalid dates such as negatives or zeroes --
SELECT
	sls_order_dt						-- Do this for sls_ship_dt and sls_due_dt as well
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0					-- Result: No negatives. But 17 rows of zeroes

SELECT
	sls_ship_dt						
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0					-- Result: None. That is good

SELECT
	sls_due_dt						
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0					-- Result: None. That is good. Only sls_order_dt has a problem which we can fix --

-- Handle 0 dates as NULL --
SELECT
	NULLIF(sls_order_dt, 0)	AS sls_order_dt			-- Assigns NULLIF sls_order_dt value is 0 --
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0

-- Now let's fix the date format of the three date columns. By first counting the length of the values --
SELECT
	NULLIF(sls_order_dt, 0)	AS sls_order_dt		-- Assigns NULLIF sls_order_dt value is 0 --
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
	OR LEN(sls_order_dt) != 8					-- Since there are a total of 8 chars in YYYYMMDD. Result: 2 rows--
	OR sls_order_dt > 20500101					-- 20500101 is a upper boundary test to make sure no dates are at, for example, in the year 2050 --
	OR sls_order_dt < 19000101					-- 19000101 is a lower boundary test to make sure no dates are at, for example, in the year 1900 --

SELECT
	sls_ship_dt		
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 
	OR LEN(sls_ship_dt) != 8					
	OR sls_ship_dt > 20500101	
	OR sls_ship_dt < 19000101					-- Result: None. That is good --
	
SELECT
	 sls_due_dt			
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
	OR LEN(sls_due_dt) != 8	
	OR sls_due_dt > 20500101	
	OR sls_due_dt < 19000101					-- Result: None. So only sls_order_dt again has a problem of wrong date lengths --

-- So again, let's improve sls_order_dt to give NULL if the date is 0 or has a length not equal to 8
SELECT
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		 ELSE sls_order_dt
		 END AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
	OR LEN(sls_order_dt) != 8					
	OR sls_order_dt > 20500101					
	OR sls_order_dt < 19000101

-- Finally let's convert these INTeger 'dates' into actual DATE datatypes. --
SELECT
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)		-- In SMSS, you can't do INT -> DATE. You first have to convert INT -> VARCHAR then -> DATE --
		 END AS sls_order_dt
FROM bronze.crm_sales_details

-- Those tests done, let us fix it in the main cleaning script --
SELECT
	sls_ord_num,
	sls_prd_key, 
	sls_cust_id, 
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) !=8 THEN NULL 
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		 END AS sls_order_dt,			
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) !=8 THEN NULL -- Even if there are no problems for sls_ship_dt let us do the same cleaning just in case --
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		 END AS sls_ship_dt,
	CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) !=8 THEN NULL	-- Same for sls_due_dt. Just as a precaution for future data entries --
		 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		 END AS sls_due_dt,
	sls_sales, 
	sls_quantity, 
	sls_price
FROM bronze.crm_sales_details

-- There is however one more data quality check to do for the dates and that is that sls_order_dt must always be earlier than sls_ship_dt and sls_due_dt --
-- It won't make sense for a shipping date or a due date to be earlier than when an order is made --

-- Check for invalid order dates --

SELECT
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt
FROM bronze.crm_sales_details
WHERE  sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt		-- Result: None. That is good --

-- Now let's move on to the last three columns of the crm_sales_details table: sls_sales, sls_quantity, sls_price --

-- These values should not be Negative, Zeroes, or Nulls, and quantity * price should be equal to the sales, as a Business Rules --
SELECT DISTINCT
	sls_sales,											
	sls_quantity,															-- sls_quantity shows to have no problems --
	sls_price											
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price 
	OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
	OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0				-- 33 total rows of invalid values from sls_sales and sls_price --
ORDER BY sls_sales, sls_quantity, sls_price

-- Before fixing this, it is good to talk and consult with an expert, maybe someone who handles business data or source systems manager --

-- But for this project, we will just fix it with our own defined rules for now --

/*
- If sls_sales is negative, zero, or null, derive it using the quantity and price: sls_sales = sls_price * sls_quantity 
- If sls_price has zero or null, derive it using the sales and quantity: sales_price = sls_sales / sls_quantity
- If sls_price is negative, convert to positive value 
*/

SELECT DISTINCT
	sls_sales AS old_sls_sales,
	sls_quantity,	
	sls_price old_sls_price,
	CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,
	CASE WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity,0) -- Not allowing to divide by 0 --
		 ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price 
	OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
	OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0				-- 33 total rows of invalid values from sls_sales and sls_price --
ORDER BY sls_sales, sls_quantity, sls_price

-- Now let's combine this with the main cleaning script --
SELECT
	sls_ord_num,
	sls_prd_key, 
	sls_cust_id, 
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) !=8 THEN NULL 
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		 END AS sls_order_dt,			
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) !=8 THEN NULL 
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		 END AS sls_ship_dt,
	CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) !=8 THEN NULL	
		 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		 END AS sls_due_dt,
	CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,							-- Replaced old sls_sales
	sls_quantity,								-- Keep current sls_quantity
	CASE WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity,0) 
		 ELSE sls_price							-- Replace old sls_price
	END AS sls_price
FROM bronze.crm_sales_details

-- Before inserting this to the silver.table, let us first check its DDL if it coincides with the current table --
IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL	
	DROP TABLE silver.crm_sales_details;
GO
CREATE TABLE silver.crm_sales_details
(
	sls_ord_num	NVARCHAR(50),						-- This is fine --
	sls_prd_key	NVARCHAR(50),						-- This is fine --
	sls_cust_id	INT,								-- This is fine --
	sls_order_dt DATE,								-- This one is not fine since we changed INT to DATE --
	sls_ship_dt	DATE,								-- Changed from INT to DATE --
	sls_due_dt DATE,								-- Changed from INT to DATE --
	sls_sales INT,
	sls_quantity INT,	
	sls_price INT,
	-- NEW in Silver Layer --
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- With that double checked, we can now insert the cleaned table to its silver variant --
INSERT INTO silver.crm_sales_details
(
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
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) !=8 THEN NULL 
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		 END AS sls_order_dt,			
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) !=8 THEN NULL 
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		 END AS sls_ship_dt,
	CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) !=8 THEN NULL	
		 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		 END AS sls_due_dt,
	CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,							-- Replaced old sls_sales
	sls_quantity,								-- Keep current sls_quantity
	CASE WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity,0) 
		 ELSE sls_price							-- Replace old sls_price
	END AS sls_price
FROM bronze.crm_sales_details

-- Quality checking the silver table --

-- Checking invalid dates --
SELECT
	*					
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt		-- Result: None. That is good --

-- Check data consistency for sales, quantity, and price. Should have no nulls, zeroes, or negatives --

SELECT DISTINCT
	sls_sales,
	sls_quantity,	
	sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price 
	OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
	OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0				-- Result: None. That is good --
ORDER BY sls_sales, sls_quantity, sls_price

-- crm_sales_details data cleaning done --

SELECT * FROM silver.crm_sales_details