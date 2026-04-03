/*
======================================================================
Descriere Stored Procedure to populate data in SILVER LAYER bla blabla
======================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
	BEGIN
		DECLARE @starttime DATETIME, @endtime DATETIME;
		BEGIN TRY
			SET @starttime = GETDATE();
			--INSERT INTO THE SILVER LAYER
			PRINT '>>Inserting Data into: silver layer'

			--crm_cust_info
			PRINT '>>TRUNCATING Data from: silver.crm_cust_info'
			TRUNCATE TABLE silver.crm_cust_info
			PRINT '>>Inserting Data into: silver layer'
			INSERT INTO silver.crm_cust_info (
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_material_status,
				cst_gndr,
				cst_create_date)
			SELECT
				cst_id,
				cst_key,
				TRIM(cst_firstname) as cst_firstname,
				TRIM(cst_lastname) as cst_lastname,
					CASE
					WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
					WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
					ELSE 'n/a' --Handling missing values, filling in blanks by adding a default value
				END cst_material_status, --Normalize gender values to readable format
				CASE
					WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
					WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
					ELSE 'n/a' --Handling missing values, filling in blanks by adding a default value
				END cst_gndr, --Normalize gender values to readable format
				cst_create_date
			FROM(
				select 
				*,
				ROW_NUMBER() OVER(partition by cst_id order by cst_create_date desc) as flag_last --Select only the most recent, relevant value for customer (= removing duplicates)
				FROM bronze.crm_cust_info
				WHERE cst_id IS NOT NULL
			)t
			WHERE flag_last = 1;--Select the most recent record per customer;



			--crm_prd_info
			TRUNCATE TABLE silver.crm_prd_info
			INSERT silver.crm_prd_info (
				prd_id,
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
				REPLACE(SUBSTRING(prd_key, 1,5),'-', '_') as cat_id, --Extract category ID
				SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key, --Extract Product Key
				prd_nm,
				ISNULL(prd_cost,0) as prd_cost,
				CASE 
					WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
					WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Roar'
					WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
					WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
				END prd_line, -- Map product line codes to descriptive values
				CAST(prd_start_dt as DATE),
				CAST(LEAD(prd_start_dt) OVER(Partition by prd_key order by prd_start_dt asc)-1 as DATE) prd_end_dt -- Calculate end date as one day before the next start date
			FROM bronze.crm_prd_info


			--crm_sales_details
			TRUNCATE TABLE silver.crm_sales_details
			INSERT INTO silver.crm_sales_details (
				sls_ord_num ,
				sls_prd_key ,
				sls_cust_id,
				sls_order_dt ,
				sls_ship_dt ,
				sls_due_dt ,
				sls_sales ,
				sls_quantity ,
				sls_price 
			)
			SELECT 
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				CASE
					WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN null
					ELSE CAST(CAST(sls_order_dt as VARCHAR) as DATE)
				END	sls_order_dt,
				CASE
					WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN null
					ELSE CAST(CAST(sls_ship_dt as VARCHAR) as DATE)
				END	sls_ship_dt,
				CASE
					WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN null
					ELSE CAST(CAST(sls_due_dt as VARCHAR) as DATE)
				END	sls_due_dt,
				CASE 
					WHEN sls_sales <=0 OR sls_sales is null OR sls_sales!=sls_quantity*ABS(sls_price) THEN sls_quantity*sls_price
					ELSE sls_sales
				END	sls_sales,
				sls_quantity,
				CASE
					WHEN sls_price is NULL OR sls_price <=0 THEN sls_sales/NULLIF(sls_quantity,0)
					ELSE sls_price
				END	sls_price
			FROM bronze.crm_sales_details;


			--erp_cust_az12
			TRUNCATE TABLE silver.erp_cust_az12
			INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
			select
				CASE 
					WHEN cid like 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
					ELSE cid
				END cid,
				CASE
					WHEN bdate > GETDATE() THEN null
					ELSE bdate
				END bdate,
				CASE
					WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
					WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
					ELSE 'n/a'
				END	gen
			FROM bronze.erp_cust_az12;


			--erp_loc_a101
			TRUNCATE TABLE silver.erp_loc_a101
			INSERT INTO silver.erp_loc_a101 (cid, cntry)
			select 
				REPLACE(cid,'-', '') cid,
				CASE 
					WHEN TRIM(cntry) = 'DE' THEN 'Germany'
					WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
					WHEN TRIM(cntry) = '' OR cntry is null THEN 'n/a'
				ELSE TRIM(cntry)
				END cntry
			FROM bronze.erp_loc_a101;


			--erp_px_cat_g1v2
			TRUNCATE TABLE silver.erp_px_cat_g1v2
			INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
			select 
				id,
				cat,
				subcat,
				maintenance
			FROM bronze.erp_px_cat_g1v2
			SET @endtime = GETDATE();
			PRINT '<<Load Duration:'+ CAST(DATEDIFF(SECOND, @starttime, @endtime) as NVARCHAR) + ' seconds.';
		END TRY
		BEGIN CATCH
			PRINT '==============================================================='
			PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
			PRINT 'Error Message:' + ERROR_MESSAGE();
			PRINT 'Error Message:' + CAST (ERROR_NUMBER() as NVARCHAR);
			PRINT 'Error Message:' + CAST(ERROR_STATE() as NVARCHAR);
			PRINT '==============================================================='
		END CATCH
END

