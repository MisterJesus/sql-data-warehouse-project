-- Chapter 12.1.6.1 - Data Warehouse (Gold Layer - Creating Dimension Customer) --

-- After Exploring and Understanding our Business Objects (See Integration Model for DataWarehousing Project) --
-- We will now do joining --

-- First for the Customers Objects --

SELECT 
	ci.cst_id,
	ci.cst_key,
	cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	cl.cntry
FROM silver.crm_cust_info AS ci				-- Master Table: Customer Information --
LEFT JOIN silver.erp_cust_az12 AS ca		-- Joined Table: Extra Customer Information --
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS cl			-- Joined Table: Customer Country Location --
ON ci.cst_key = cl.cid

-- After joining, check if any duplicates were introduced by the join logic --

SELECT cst_id, COUNT(*)
FROM
(
SELECT 
	ci.cst_id,
	ci.cst_key,
	cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	cl.cntry
FROM silver.crm_cust_info AS ci		
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS cl
ON ci.cst_key = cl.cid
)t GROUP BY cst_id
HAVING COUNT(*) > 1						-- No result. That is good. No duplicates --

-- Check for columns providing the same information. In this case the gender in crm_cust_info and the gender in erp_cust_az12 --

SELECT DISTINCT
	ci.cst_gndr,						
	ca.gen
FROM silver.crm_cust_info AS ci			
LEFT JOIN silver.erp_cust_az12 AS ca	
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS cl
ON ci.cst_key = cl.cid
ORDER BY 1, 2							-- Note: If columns aren't given aliases, you can use numbers to specify the order --

/*
Even though we cleaned both tables, we are getting NULL values. Don't worry, those NULL values are
caused by joining the tables. But as we can see, the DISTINCT values are not matching.
Meaning there are data mismatch. We normally would discuss with the source manager what to keep
But for this case we will mainly keep the master table information, 
and output the erp gender table if the value in the master table isn't available
*/

-- Let's perform Data Integration --

SELECT DISTINCT
	ci.cst_gndr,						
	ca.gen,								
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
		 ELSE COALESCE(ca.gen, 'n/a')
	END AS new_gen
FROM silver.crm_cust_info AS ci			
LEFT JOIN silver.erp_cust_az12 AS ca	
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS cl
ON ci.cst_key = cl.cid
ORDER BY 1, 2	

-- Let us replace the old gender columns with the newly data integrated one --

SELECT 
	ci.cst_id,
	ci.cst_key,
	cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
		 ELSE COALESCE(ca.gen, 'n/a')
	END AS new_gen,
	ci.cst_create_date,
	ca.bdate,
	cl.cntry
FROM silver.crm_cust_info AS ci		
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS cl
ON ci.cst_key = cl.cid

-- Now let us rename the columns to User-Friendly Names --

SELECT 
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
		 ELSE COALESCE(ca.gen, 'n/a')
	END AS gender,
	ci.cst_create_date AS create_date,
	ca.bdate AS birthdate,
	cl.cntry AS country
FROM silver.crm_cust_info AS ci		
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS cl
ON ci.cst_key = cl.cid

-- Next, let's sort the columns in logical groups to improve readability --

SELECT 
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	cl.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
		 ELSE COALESCE(ca.gen, 'n/a')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci		
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS cl
ON ci.cst_key = cl.cid

-- Now the decision. Is this a Dimension Table or Fact Table? --

/*
Dimension Table is descriptive information that gives context of your data. Answers Who, Where, What
Fact Table is quantitative information that represents events. Answers How much, How many, etc.
*/
-- The answer is Dimension Table, as this table focuses on describing the customer. --
-- So we will call this table the dimension customer or dim_customers --
-- And dimension tables need a primary key. But we can use a surrogate key instead --

SELECT
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	cl.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
		 ELSE COALESCE(ca.gen, 'n/a')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci		
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS cl
ON ci.cst_key = cl.cid

-- Now we can create this object. And the objects in the Gold Layer are virtual. So we will use VIEWS --
-- Remember views can be found in: -- Server > Databases > DataWarehouse > Views > --

CREATE VIEW gold.dim_customers AS			
SELECT
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	cl.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
		 ELSE COALESCE(ca.gen, 'n/a')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci		
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS cl
ON ci.cst_key = cl.cid

-- After creating this VIEW, time to do a quality check --

-- Gender uniqueness --

SELECT DISTINCT gender FROM gold.dim_customers		-- Only three values. N/A, Male, and Female. That is good compared to earlier results --

