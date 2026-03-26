-- Chapter 12.1.4.2 - Data Warehouse (Silver Layer - Data Cleaning and Quality Checking: crm_prd_info ) --
-- However we will use the bronze.table for cleaning first before inserting it to its silver.table variant --

-- First we study the table to see what kind of cleaning and quality checking we can do --
SELECT 
	prd_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info

-- prd_key can be splitted into two to grab the catalog_id. The point of the splits is so that it can be joined with other tables --
SELECT 
	prd_id,
	prd_key,
	SUBSTRING(prd_key, 1, 5) AS cat_id,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info

SELECT DISTINCT ID FROM bronze.erp_px_cat_g1v2		-- The cat_id is same as the id in this table here --

-- Fix cat_id so that it matches the ID of the erp_px_cat_g1v2 table and check for cat_id values not in erp_px_cat_g1v2 --
SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN	-- This is to filter out unmatched data after applying the transformation --
	(SELECT DISTINCT ID FROM bronze.erp_px_cat_g1v2)		-- And based on the result, the cat_id 'CO_PE' is not in the erp_px_cat_g1v2 table --

-- Now split the other part of the prd_key and remove old prd_key --
SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key))	AS prd_key,	-- Since the end length varies, let us just assign the end value as LEN(prd_key) so it is dynamic --
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info

SELECT sls_prd_key FROM bronze.crm_sales_details	-- This is the column and table where we will join the splitted prd_key from --

-- Check values NOT IN sls_prd_key with the splitted prd_key -- 
SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key))	AS prd_key,	-- Since the end length varies, let us just assign the end value as LEN(prd_key) so it is dynamic --
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info 
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN			-- The many outputs are the products without any orders --
	(SELECT sls_prd_key FROM bronze.crm_sales_details)

-- Next is the prd_nm. We can check for any unwanted spaces with it --
SELECT 
	prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)		-- Clean. No problems there. --

-- Next is the prd_cost. Let us check for the quality of the numbers --
SELECT
	prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL		-- Checking for NULLs and negative numbers. Result: There are two NULLS no negative numbers --

-- So to handle those NULLs we can COALESCE to the the column prd_cost --
SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key))	AS prd_key,
	prd_nm,
	COALESCE(prd_cost, 0) AS prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info

-- Next is prd_line, which has poor data quality since it is in abbreviations. So let us fix it. --
SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key))	AS prd_key,
	prd_nm,
	COALESCE(prd_cost, 0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))			-- Since we will be using UPPER(TRIM(prd_line)) a lot. Instead of doing it on every WHEN we can do a shortcut --
		WHEN 'M' THEN 'Mountain'		
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'N/A'
	END AS prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info

-- Last two we have the prd_start_dt an prd_end_dt. The start and end dates. For this we can check for invalid dates --
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt		-- We are checking for end dates that are smaller than the start dates, since that is invalid. Result: 200 rows --

-- Fixing this data will be tricky as the solution is simply not switching the start and end dates. The data must stay logical --

/* 
For this one, you will have to do some tests on the invalid data on how you want to approach it. 
We can't simply delete those rows at they may be crucial data.
We can't just swap them since there are huge inconsistencies in between years for succeeding entries.
This is something that you would normally talk about with your system source manager.
But for now, how we're going to fix it is by turning the LEAD prd_start_dt (the next value partitioned by prd_key) as our prd_end_dt
*/

SELECT
	prd_id,
	prd_key,
	prd_nm,
	prd_start_dt,
	prd_end_dt,
	LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS prd_end_dt_test -- Subtract a day so it won't overlap with the prd_start_dt --
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509') -- Let's grab a few examples of invalid dates -- 

/* Satisfied with that fix, let us put it on the main cleaning script. Replacing prd_end_dt. 
And at the same time, let's convert DATETIME for those columns into DATE */
SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key))	AS prd_key,
	prd_nm,
	COALESCE(prd_cost, 0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))			-- Since we will be using UPPER(TRIM(prd_line)) a lot. Instead of doing it on every WHEN we can do a shortcut --
		WHEN 'M' THEN 'Mountain'		
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'N/A'
	END AS prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info

-- Before we can insert this cleaned data. Remember we added some columns and changed the date datatypes. So we need to alter this in the silver.table --
-- This happens when we're fixing the data quality of bronze.tables. We will adjust and fix the silver.tables metadata to coincide with the cleaned table --
IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info
(
	prd_id INT,
	cat_id NVARCHAR(50),				-- New Column --
	prd_key	NVARCHAR(50),				-- Removed old prd_key for this new prd_key --
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,					-- Changed from DATETIME to DATE --
	prd_end_dt DATE,					-- Changed from DATETIME to DATE --
	-- NEW in Silver Layer --
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);	-- Run this query above and it should update the table with the new columns and updated datatypes --

-- Now we can INSERT this cleaned bronze.crm_prd_info to its silver.crm_prd_info variant --
INSERT INTO silver.crm_prd_info
(
	prd_id,							-- Always remember to match the table columns to the table columns you are inserting to --
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key))	AS prd_key,
	prd_nm,
	COALESCE(prd_cost, 0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))			-- Since we will be using UPPER(TRIM(prd_line)) a lot. Instead of doing it on every WHEN we can do a shortcut --
		WHEN 'M' THEN 'Mountain'		
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'N/A'
	END AS prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info

-- crm_prd_info cleaning DONE --

-- Now double checking the data quality --
SELECT 
	prd_id,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1	OR prd_id IS NULL			-- Result is none. That is good --

-- No unwanted spaces --
SELECT 
	prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)					-- Result is none. That is good --

-- No NULLs or Negative Numbers --
SELECT 
	prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 AND prd_cost IS NULL			-- Result is none. That is good --

-- Clearer values. Instead of abbreviations --
SELECT DISTINCT
	prd_line									
FROM silver.crm_prd_info;						-- Result is cleaner and no abbreviations. That is good --

-- Check for Invalid Date Orders --
SELECT 
	*
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt					-- Result is none. That is good --

SELECT * FROM silver.crm_prd_info