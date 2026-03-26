-- Chapter 12.1.4.5 - Data Warehouse (Silver Layer - Data Cleaning and Quality Checking - erp_loc_a101) --
-- However we will use the bronze.table for cleaning first before inserting it to its silver.table variant --

-- Check the table. Only two columns --
SELECT
	cid, 
	cntry
FROM bronze.erp_loc_a101

-- Again in the data integration we saw that we can connect cid to cst_key of crm_cust_info too --
SELECT
	cid							-- The output now has a hyphen between the id --
FROM bronze.erp_loc_a101

-- Let's fix that --
SELECT
	REPLACE(cid, '-', '') AS cid		-- Replace the hyphen with a blank --
FROM bronze.erp_loc_a101

-- Check test-connectivity with crm_cust_info --
SELECT
	REPLACE(cid, '-', '') AS cid		
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN (SELECT cst_key FROM silver.crm_cust_info)		-- Result: None. Meaning no unmatching data. That is good --

-- Next country --
SELECT DISTINCT
	cntry						-- Has NULLs, abbreviations, blanks, three versions of US (US, USA, United States) --
FROM bronze.erp_loc_a101

-- Let's fix that --
SELECT DISTINCT
	cntry,
	CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'N/A'
		ELSE TRIM(cntry)
	END AS new_cntry
FROM bronze.erp_loc_a101
ORDER BY cntry

-- Combine it with the main cleaning script --
SELECT
	REPLACE(cid, '-', '') AS cid,
	CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'N/A'
		ELSE TRIM(cntry)
	END AS new_cntry
FROM bronze.erp_loc_a101

-- Double check the silver.table metadata before inserting the cleaned values --
IF OBJECT_ID ('silver.erp_loc_a101', 'U') IS NOT NULL	
	DROP TABLE silver.erp_loc_a101;
GO
CREATE TABLE silver.erp_loc_a101
(
	CID NVARCHAR(50),									-- No changes --
	CNTRY NVARCHAR(50),									-- No changes --
	-- NEW in Silver Layer --
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- We can insert it now --
INSERT INTO silver.erp_loc_a101
(
	cid,
	cntry
)
SELECT
	REPLACE(cid, '-', '') AS cid,
	CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'N/A'
		ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101

-- Data quality check for silver.table --

-- No hyphens for cid --
SELECT
	cid
FROM silver.erp_loc_a101

-- Country data standardization --
SELECT DISTINCT
	cntry
FROM silver.erp_loc_a101

-- silver.erp_loc_a101 cleaning finished --

SELECT * FROM silver.erp_loc_a101