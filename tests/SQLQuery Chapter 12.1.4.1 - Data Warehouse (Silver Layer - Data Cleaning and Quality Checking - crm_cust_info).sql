-- Chapter 12.1.4.1 - Data Warehouse (Silver Layer - Data Cleaning and Quality Checking: crm_cust_info ) --
-- However we will use the bronze tables for this --

-- CHECK for Nulls or Duplicates in Primary Key --
-- Expectation: No result --

SELECT 
	cst_id,
	COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1	OR cst_id IS NULL			-- This query checks if the PK is unique and not null--

/*
Result: 
cst_ id | Count
29449	2
29473	2
29433	2
NULL	3
29483	2
29466	3
*/			-- Duplicates and Null present, we have to fix that --

-- Let's focus on one of the duplicates so we can study on how we can fix and write a query for it --
SELECT 
	*
FROM bronze.crm_cust_info
WHERE cst_id = 29466			-- Based on these results, it is best to grab the latest date since it is complete --

/* 
Grab latest date in the form of ranking. Filter so that cst_id IS NOT NULL
Then further filtering the main query to only grab rows where rank is equal to 1
The output should now remove duplicates and NULL values of the Primary Key cst_id
*/

SELECT
	*
FROM
(
	SELECT 
		*,
		RANK() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS latest_date
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE latest_date = 1

-- Check for unwanted spaces --
SELECT 
	cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)	-- Checks rows where the firstname is not the same as a TRIMmed firstname --
											-- Check for every row and keep track of columns with results --

-- Let's combine this query with query that filters primary keys --
SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
FROM
(
	SELECT 
		*,
		RANK() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS latest_date
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE latest_date = 1

-- Now we have a query that removes duplicates and NULLs in Primary Key column and TRIMming of unwanted spaces for the first and last names --

-- Next is Data Standardization & Consistency --
SELECT DISTINCT
	cst_gndr					-- Also check the column cst_marital_status --
FROM bronze.crm_cust_info;		-- NULL is fine for gender, but let's say we want the full and clear value instead of abbreviations --

-- So now let's replace cst_gndr column with a CASE for cst_gndr in our data cleaning script to extend the abbreviations. That is the new cst_gndr column --
SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	cst_marital_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'		-- add UPPER and TRIM as well just to be sure --
		 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		 ELSE 'N/A'
	END AS cst_gndr,
	cst_create_date
FROM
(
	SELECT 
		*,
		RANK() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS latest_date
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE latest_date = 1

-- Let's do the same for cst_marital_status, extending the abbreviations -- 
SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'		-- add UPPER and TRIM as well just to be sure --
		 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		 ELSE 'N/A'
	END AS cst_marital_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'		
		 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		 ELSE 'N/A'
	END AS cst_gndr,
	cst_create_date
FROM
(
	SELECT 
		*,
		RANK() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS latest_date
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE latest_date = 1

-- Lastly the cst_create_date. But since we defined this column earlier as DATE there is no need to double check --
-- Now we can insert this cleaned bronze.table to its respective silver.table form --

INSERT 
INTO silver.crm_cust_info 
(
	cst_id,							-- MAKE SURE that the silver.table columns coincides with the bronze.table columns to avoid mismatch -- 
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
)
SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'		-- add UPPER and TRIM as well just to be sure --
		 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		 ELSE 'N/A'
	END AS cst_marital_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'		
		 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		 ELSE 'N/A'
	END AS cst_gndr,
	cst_create_date
FROM
(
	SELECT 
		*,
		RANK() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS latest_date
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE latest_date = 1

-- crm_cust_info cleaning DONE --

-- Rerunning earlier tests to check quality. But instead of the bronze.table we will do it to the silver.table --
-- Primary Key uniqueness -- 

SELECT 
	cst_id,
	COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1	OR cst_id IS NULL			-- Result is none. That is good --
-- No unwanted spaces --
SELECT 
	cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)		-- Result is none. That is good --
-- Clearer values. Instead of abbreviations --
SELECT DISTINCT
	cst_gndr									-- Also check the column cst_marital_status --
FROM silver.crm_cust_info;						-- Result is cleaner and no abbreviations. That is good --

SELECT * FROM silver.crm_cust_info				-- Silver table is clean and also outputs the metadata column: dwh_create_date --