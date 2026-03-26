-- Chapter 12.1.4.6 - Data Warehouse (Silver Layer - Data Cleaning and Quality Checking - erp_px_cat_g1v2) --
-- However we will use the bronze.table for cleaning first before inserting it to its silver.table variant --

-- Check the table. Has four columns --
SELECT
	id, 
	cat, 
	subcat, 
	maintenance
FROM bronze.erp_px_cat_g1v2

/* 
First the id, and based on the data integration we did, we will connect this id with the crm_prd_info prd_key
But rememeber that we created a catalogue id (cat_id) in the crm_prd_info that we will use instead of the prd_key to connect both tables
And we already tested their connectivity in the crm_prd_info table. So no problems for the id
*/

SELECT
	id
FROM bronze.erp_px_cat_g1v2

SELECT cat_id FROM silver.crm_prd_info

-- Next is cat (catalogue). Let's check for unwanted spaces --
SELECT
	cat
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat)				-- Result: None. That is good. There are no unwanted spaces --

SELECT
	subcat							-- Check for column subcat, too --
FROM bronze.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat)		-- Result: None. That is good. No unwanted spaces --

SELECT
	maintenance							-- Check for column maintenance, too --
FROM bronze.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance)   -- Result: None. That is good. No unwanted spaces --

-- Next let's check for data standardization & consistency --
SELECT DISTINCT
	cat
FROM bronze.erp_px_cat_g1v2			-- Result: No problems. And values are clear --

SELECT DISTINCT
	subcat
FROM bronze.erp_px_cat_g1v2			-- Result: No problems. And values are clear 

SELECT DISTINCT
	maintenance
FROM bronze.erp_px_cat_g1v2			-- Result: No problems. And values are clear --

-- This table has really good data quality. No cleanup needed. But we still need to load it to the silver.table --

INSERT INTO silver.erp_px_cat_g1v2
(
	id, 
	cat, 
	subcat, 
	maintenance
)
SELECT
	id, 
	cat, 
	subcat, 
	maintenance
FROM bronze.erp_px_cat_g1v2

-- Since there are no changes and cleaning done. No need for double checking as well --

-- erp_px_cat_g1v2 table done --
SELECT * FROM silver.erp_px_cat_g1v2