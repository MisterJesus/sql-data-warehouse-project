-- Chapter 12.1.4.4 - Data Warehouse (Silver Layer - Data Cleaning and Quality Checking - erp_cust_az12) --
-- However we will use the bronze.table for cleaning first before inserting it to its silver.table variant --

-- Check the table. Only three columns --
SELECT
	cid, 
	bdate, 
	gen
FROM bronze.erp_cust_az12

-- In the data integration we saw that the cid can be connected to the crm_cust_info's cst_key. So let's see if they can connect without problems --
SELECT
	cid, 
	bdate, 
	gen
FROM bronze.erp_cust_az12

SELECT cst_key FROM silver.crm_cust_info		-- Executing both statements, we can see that cid has a few characters different from cst_key --

-- Let's fix the cid so that it can be connected to cst_key --
SELECT
	cid,
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))		-- Some cid does not start with NAS so we can make a case --
		 ELSE cid
	END AS new_cid,
	bdate, 
	gen
FROM bronze.erp_cust_az12

-- Now let's try a test-connection with crm_cust_info. Result should be empty --
SELECT
	cid,
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END AS new_cid,
	bdate, 
	gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	  END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)	-- Result: None. That is good. Means there are no unmatching data between columns --


-- Next bdate --
SELECT
	bdate						-- Has correct data type. Let's check for boundaries  
FROM bronze.erp_cust_az12
WHERE bdate < '1920-01-01' OR bdate > GETDATE()		-- Seems we have customers that are older than 100+ and customers that aren't even born yet --

-- Let's fix that --
SELECT
	CASE WHEN bdate > GETDATE() THEN NULL			-- Case for when birthdate is greater than current date is NULL --
		 WHEN bdate < '1900-01-01' THEN NULL		-- Case for when birthdate is over a 120+ years old. Who knows, could still be valid --
		 ELSE bdate
	END AS bdate
FROM bronze.erp_cust_az12

-- Combine it in the main cleaning script --
SELECT
	cid,
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END AS new_cid,
	CASE WHEN bdate > GETDATE() THEN NULL			
		 WHEN bdate < '1900-01-01' THEN NULL
		 ELSE bdate
	END AS bdate,
	gen
FROM bronze.erp_cust_az12

-- Next let's check gen --
SELECT DISTINCT 
	gen							-- Bad. Has NULL, F, M, Blank, and then the desired outcome of Male and Female --
FROM bronze.erp_cust_az12

-- Let's fix that --
SELECT DISTINCT 
	gen,
	CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
		 ELSE 'N/A'
	END AS new_gen
FROM bronze.erp_cust_az12

-- Combine it with the main cleaning script --
SELECT
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END AS cid,
	CASE WHEN bdate > GETDATE() THEN NULL			
		 WHEN bdate < '1900-01-01' THEN NULL
		 ELSE bdate
	END AS bdate,
	CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
		 ELSE 'N/A'
	END AS gen
FROM bronze.erp_cust_az12

-- Check with the silver.table metadata if columns coincide before inserting --
IF OBJECT_ID ('silver.erp_cust_az12', 'U') IS NOT NULL	
	DROP TABLE silver.erp_cust_az12;
GO
CREATE TABLE silver.erp_cust_az12
(
	CID NVARCHAR(50),						-- No changes --
	BDATE DATE,								-- No changes --
	GEN NVARCHAR(10),						-- No changes --
	-- NEW in Silver Layer --
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- After double checking we can now insert the bronze.table to its silver.variant --
INSERT INTO silver.erp_cust_az12
(
	cid, 
	bdate, 
	gen
)
SELECT
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END AS cid,
	CASE WHEN bdate > GETDATE() THEN NULL			
		 WHEN bdate < '1900-01-01' THEN NULL
		 ELSE bdate
	END AS bdate,
	CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
		 ELSE 'N/A'
	END AS gen
FROM bronze.erp_cust_az12

-- Checking data quality of silver.table --

-- Check for invalid dates --
SELECT
	bdate						
FROM silver.erp_cust_az12
WHERE bdate < '1920-01-01' OR bdate > GETDATE() -- There are still 8 below 1920, but that is fine, we're more so checking the users that is born in the future --

-- Gender standardization --
SELECT DISTINCT 
	gen
FROM silver.erp_cust_az12			-- Now only has Female, Male, and N/A. That is good --

-- silver.erp_cust_az12 table done --
SELECT * FROM silver.erp_cust_az12