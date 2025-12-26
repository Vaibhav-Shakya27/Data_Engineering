/*
----------------------------------
The script creates a proc which, when executed loads data to all crm and erp tables


----------------------------------
*/
CREATE OR ALTER PROCEDURE bronze.sp_load_bronze as 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	DECLARE @batchstart_time DATETIME, @batchend_time DATETIME;

	BEGIN TRY
	SET @batchstart_time = GETDATE();

		PRINT '=============================================='
		PRINT 'LOADING THE BRONZE LAYER';
		PRINT '=============================================='

		PRINT 'TRUNCATING AND LOADING CRM TABLES';
		-- CRM TABLES
		PRINT '-----------------------------------------------'
		SET @start_time = GETDATE();

		PRINT 'TRUNCATING AND LOADING bronze.crm_cust_info';
		
		TRUNCATE TABLE bronze.crm_cust_info;

		bulk insert bronze.crm_cust_info
		from 'C:\Users\vaibh\OneDrive\Desktop\Projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',' ,
			tablock
		);
		SET @end_time = GETDATE();
		PRINT 'DURATION: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR ) + ' seconds';

		------------------

		PRINT '-----------------------------------------------'
		SET @start_time = GETDATE();
		PRINT 'TRUNCATING AND LOADING bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		bulk insert bronze.crm_prd_info
		from 'C:\Users\vaibh\OneDrive\Desktop\Projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',' ,
			tablock
		);
		SET @end_time = GETDATE();
		PRINT 'DURATION: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR ) + ' seconds';
		
		------------------

		PRINT '-----------------------------------------------'
		SET @start_time = GETDATE();
		PRINT 'TRUNCATING AND LOADING bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		bulk insert bronze.crm_sales_details
		from 'C:\Users\vaibh\OneDrive\Desktop\Projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',' ,
			tablock
		);

		SET @end_time = GETDATE();
		PRINT 'DURATION: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR ) + ' seconds';
		
		------------------

		PRINT '================================================'
		PRINT 'TRUNCATING AND LOADING ERP TABLES';

		-- ERP TABLES

		PRINT '==============================================='

		SET @start_time = GETDATE();

		PRINT 'TRUNCATING AND LOADING bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		bulk insert bronze.erp_cust_az12
		from 'C:\Users\vaibh\OneDrive\Desktop\Projects\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',' ,
			tablock
		);

		SET @end_time = GETDATE();
		
		PRINT 'DURATION: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR ) + ' seconds';
		
		------------------
		PRINT '-----------------------------------------------'
		SET @start_time = GETDATE();
		PRINT 'TRUNCATING AND LOADING bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		bulk insert bronze.erp_loc_a101
		from 'C:\Users\vaibh\OneDrive\Desktop\Projects\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',' ,
			tablock
		);

		SET @end_time = GETDATE();
		PRINT 'DURATION: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR ) + ' seconds';
		
		------------------

		PRINT '-----------------------------------------------'
		PRINT 'TRUNCATING AND LOADING bronze.erp_px_cat_g1v2';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\vaibh\OneDrive\Desktop\Projects\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',' ,
			tablock
		);
		SET @end_time = GETDATE();
		PRINT 'DURATION: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR ) + ' seconds';


		PRINT 'FINISHED LOADING';

		SET @batchend_time = GETDATE();

		PRINT 'BATCH DURATION : ' + CAST(DATEDIFF(second, @batchstart_time, @batchend_time) as nvarchar) +' seconds';
		PRINT '-----------------------------------------------'

	END TRY

	BEGIN CATCH
		
		PRINT '==============================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '==============================================='


	END CATCH
END
			

EXEC bronze.sp_load_bronze;
