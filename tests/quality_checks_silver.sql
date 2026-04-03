--=================================================================================
-----------Data Quality Checks performed in SILVER, before inserting in SILVER----
--=================================================================================
-- Check for Nulls or Duplicates in PKs (expectation: no result)
SELECT 
	cst_id,
	count(*)
FROM silver.crm_cust_info
GROUP By cst_id
having count(*) >1 OR cst_id is null;


-- Check for unwanted Spaces (expectation: no result): first_name, last_name etc
SELECT 
	cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);


-- Data Standardization & Consistency:
SELECT 
	distinct cst_gndr
FROM silver.crm_cust_info;
