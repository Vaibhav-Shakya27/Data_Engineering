CREATE OR ALTER PROCEDURE bronze.sp_load_bronze as 
BEGIN
	
	PRINT 'LOADING THE BRONZE LAYER';
	PRINT 'LOADING THE CRM TABLES';
	-- CRM TABLES

	PRINT 'LOADING bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;

	bulk insert bronze.crm_cust_info
	from 'C:\Users\vaibh\OneDrive\Desktop\Projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	with (
		firstrow = 2,
		fieldterminator = ',' ,
		tablock
	);


	
	PRINT 'LOADING bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;

	bulk insert bronze.crm_prd_info
	from 'C:\Users\vaibh\OneDrive\Desktop\Projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	with (
		firstrow = 2,
		fieldterminator = ',' ,
		tablock
	);

	
	PRINT 'LOADING bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;

	bulk insert bronze.crm_sales_details
	from 'C:\Users\vaibh\OneDrive\Desktop\Projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	with (
		firstrow = 2,
		fieldterminator = ',' ,
		tablock
	);


	
	PRINT 'LOADING ERP TABLES';

	-- ERP TABLES

	
	PRINT 'LOADING bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;

	bulk insert bronze.erp_cust_az12
	from 'C:\Users\vaibh\OneDrive\Desktop\Projects\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
	with (
		firstrow = 2,
		fieldterminator = ',' ,
		tablock
	);


	
	PRINT 'LOADING bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;

	bulk insert bronze.erp_loc_a101
	from 'C:\Users\vaibh\OneDrive\Desktop\Projects\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
	with (
		firstrow = 2,
		fieldterminator = ',' ,
		tablock
	);


	
	PRINT 'LOADING bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;

	bulk insert bronze.erp_cust_az12
	from 'C:\Users\vaibh\OneDrive\Desktop\Projects\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
	with (
		firstrow = 2,
		fieldterminator = ',' ,
		tablock
	);
	
	PRINT 'FINISHED LOADING';
END


EXEC bronze.sp_load_bronze;
