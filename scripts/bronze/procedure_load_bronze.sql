/*
===================================
Descriere Stored Procedure bla blabla
===================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		
	SET @batch_start_time = GETDATE();
		PRINT '========================================';
		PRINT 'Loading Bronze Layer';
		PRINT '========================================';

		SET @start_time = GETDATE();
		PRINT 'Loading CRM Tables';
		PRINT 'TRUNCATING Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info
		BULK INSERT  bronze.crm_cust_info
		FROM 'C:\Users\Iulian\Downloads\SQL Course\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds';


		TRUNCATE TABLE bronze.crm_prd_info
		BULK INSERT  bronze.crm_prd_info
		FROM 'C:\Users\Iulian\Downloads\SQL Course\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		TRUNCATE TABLE bronze.crm_sales_details
		BULK INSERT  bronze.crm_sales_details
		FROM 'C:\Users\Iulian\Downloads\SQL Course\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);


		PRINT 'Loading ERP Tables';
		TRUNCATE TABLE bronze.erp_cust_az12
		BULK INSERT  bronze.erp_cust_az12
		FROM 'C:\Users\Iulian\Downloads\SQL Course\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);


		TRUNCATE TABLE bronze.erp_loc_a101
		BULK INSERT  bronze.erp_loc_a101
		FROM 'C:\Users\Iulian\Downloads\SQL Course\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		BULK INSERT  bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Iulian\Downloads\SQL Course\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

	SET @batch_end_time = GETDATE();
	PRINT '>>BATCH Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) as NVARCHAR) + ' seconds';
	END TRY
	BEGIN CATCH
		PRINT '===========================' 
		PRINT 'ERROR OCCURED during loading bronze layer'
		PRINT 'Error message' + ERROR_MESSAGE();
		PRINT 'Error message' + CAST (ERROR_NUMBER() as NVARCHAR);
		PRINT 'Error message' + CAST (ERROR_STATE() as NVARCHAR);
		PRINT '==========================='
	END CATCH
END
