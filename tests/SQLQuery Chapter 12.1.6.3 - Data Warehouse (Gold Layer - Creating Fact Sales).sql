-- Chapter 12.1.6.3 - Data Warehouse (Gold Layer - Creating Fact Sales) --

-- In the Integration Model there is only one Sales object --

-- So no need for data integration --

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
FROM silver.crm_sales_details

/*
We can go straight to answering is this a Dimension or a Fact Table?
It's a Fact table as it answers quantitative questions
And a Fact table connects to dimensional tables. 
So we have to connect this fact table to its columns corresponding to the surrogate keys of the dimensions.
Remember to look at the Integration Model to see how and which column they are connected
*/

SELECT 
	sls_ord_num,
	pr.product_key,
	cu.customer_key,
	-- sls_prd_key,							-- Replaced by pr.product_key
	-- sls_cust_id,							-- Replaced by cu.customer_key
	sls_order_dt, 
	sls_ship_dt, 
	sls_due_dt, 
	sls_sales, 
	sls_quantity, 
	sls_price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
ON sd.sls_prd_key = pr.product_number		-- Only information we need here is the surrogate key --
LEFT JOIN gold.dim_customers AS cu
ON sd.sls_cust_id = cu.customer_id			-- Again we only need the surrogate key --

-- Because of this connection. Our fact table is now connected to its related dimensional tables -- 

-- Now let us rename the column into meaningful User-Friendly names. And order columns into logical groups for readability --

SELECT 
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt AS order_date, 
	sd.sls_ship_dt AS shipping_date, 
	sd.sls_due_dt AS due_date, 
	sd.sls_sales AS sales_amount, 
	sd.sls_quantity AS quantity, 
	sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
ON sd.sls_prd_key = pr.product_number		
LEFT JOIN gold.dim_customers AS cu
ON sd.sls_cust_id = cu.customer_id	

-- Create view as it is the gold layer --

CREATE VIEW gold.fact_sales AS				-- Name it as gold.fact_sales not gold.dim_sales. It is a fact. --
SELECT 
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt AS order_date, 
	sd.sls_ship_dt AS shipping_date, 
	sd.sls_due_dt AS due_date, 
	sd.sls_sales AS sales_amount, 
	sd.sls_quantity AS quantity, 
	sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
ON sd.sls_prd_key = pr.product_number		
LEFT JOIN gold.dim_customers AS cu
ON sd.sls_cust_id = cu.customer_id	

-- Quality checking --

-- Foreign key integrity. Checking if all dimension tables can be successfully joined -- 

SELECT									-- Check for dim_customers --
	* 
FROM gold.fact_sales AS s
LEFT JOIN gold.dim_customers AS c
ON c.customer_key = s.customer_key
WHERE c.customer_key IS NULL

SELECT									-- Same checking for dim_products
	* 
FROM gold.fact_sales AS s
LEFT JOIN gold.dim_products AS p
ON p.product_key = s.product_key
WHERE p.product_key IS NULL

-- With that we have now created three views --