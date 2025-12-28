/*
----------------------------------------------------------------
The script creates and executes a proc resoponsible for loading transformed data into the silver layer.
The data has been cleansed to have refined and cleaner data.

Data flow occurs from Bronze -> Silver layer tables post ETL (Extract, Transform and Loading)

The script truncates and loads existing tables with same name

EXEC silver.sp_load_silver;
----------------------------------------------------------------
*/


CREATE OR ALTER PROCEDURE silver.sp_load_silver AS
BEGIN
	DECLARE @batchstart_time DATETIME, @batchend_time DATETIME ;
	SET @batchstart_time = GETDATE() ;
	BEGIN TRY
	DECLARE @start_time DATETIME, @end_time DATETIME ;
	
		
		PRINT '================================'
		PRINT 'LOADING DATA FOR CRM TABLES'
		PRINT '================================'
		
		TRUNCATE TABLE silver.crm_cust_info ;
		PRINT 'LOADING DATA FOR silver.crm_cust_info'
		PRINT '--------------------------------'

		SET @start_time = GETDATE();

		insert into silver.crm_cust_info (
		cst_id,
		cst_key ,	
			cst_firstname,
			cst_lastname,
			cst_marital_status ,
			cst_gndr,
			cst_create_date )

		select cst_id ,
			cst_key ,
			trim(cst_firstname) as cst_firstname,
			trim(cst_lastname) as cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'Unknown'
			END cst_marital_status ,
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'Unknown'
			END cst_gndr,
			cst_create_date 
		from 
		(
		-- Removing non duplicate primary key values 
		select *, ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as row_num
		from bronze.crm_cust_info
		where cst_id is not null
		)t 
		where row_num = 1
		;

		SET @end_time = GETDATE();
		PRINT 'LOADED DATA, PROCESS TOOK : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds'
		PRINT '--------------------------------'
		PRINT 'LOADING DATA FOR silver.crm_prd_info'
		PRINT '--------------------------------'
		
		SET @start_time = GETDATE();

		TRUNCATE TABLE silver.crm_prd_info;
		INSERT INTO silver.crm_prd_info (
		prd_id ,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost ,
		prd_line ,
		prd_start_dt ,
		prd_end_dt 
		)
		select prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
		prd_nm,
		ISNULL(prd_cost,0) as prd_cost,
		CASE
			WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
			WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			ELSE 'Unknown'
		END prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 AS DATE) as prd_end_dt

		from bronze.crm_prd_info
		;

		SET @end_time = GETDATE();

		PRINT 'LOADED DATA, PROCESS TOOK : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds'

		PRINT '--------------------------------'
		PRINT 'LOADING DATA FOR silver.crm_sales_details'
		PRINT '--------------------------------'

		SET @start_time = GETDATE();

		TRUNCATE TABLE silver.crm_sales_details;
		insert into silver.crm_sales_details
		(sls_ord_num	,
		sls_prd_key	,
		sls_cust_id	,
		sls_order_dt ,
		sls_ship_dt	,
		sls_due_dt	,
		sls_sales ,
		sls_quantity ,
		sls_price )

		select sls_ord_num,
		sls_prd_key	,
		sls_cust_id	,
		CASE WHEN sls_order_dt = 0 or LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS varchar) AS DATE)
			END AS sls_order_dt,
		CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) as sls_ship_dt,
		CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) as sls_due_dt,

		CASE WHEN sls_sales IS NULL 
			or sls_sales <= 0 
			or sls_sales <> abs(sls_price) * sls_quantity 
			THEN abs(sls_price) * sls_quantity
			ELSE sls_sales
		END sls_sales,
		sls_quantity ,
		CASE WHEN sls_price IS NULL or sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity,0)
			ELSE sls_price
		END sls_price

		from bronze.crm_sales_details
		;

		SET @end_time = GETDATE();
		PRINT 'LOADED DATA, PROCESS TOOK : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds'

		PRINT '--------------------------------'
		PRINT 'LOADING DATA FOR silver.erp_cust_az12'
		PRINT '--------------------------------'
		SET @start_time = GETDATE();

		TRUNCATE TABLE silver.erp_cust_az12;
		insert into silver.erp_cust_az12
		(
		cid,
		bdate ,
		gen
		)

		select 
		CASE 
			WHEN cid like 'NAS%' THEN substring(cid,4,len(cid))
			ELSE cid
		END cid,
		CASE 
			WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END bdate,
		CASE 
			WHEN upper(gen) = 'M' THEN 'Male'
			WHEN upper(gen) = 'F' THEN 'Female'
			WHEN gen is null or gen = '' THEN 'Unknown'
			ELSE gen
		END gen
		from bronze.erp_cust_az12
		;

		SET @end_time = GETDATE();
		PRINT 'LOADED DATA, PROCESS TOOK : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds'

		PRINT '--------------------------------'
		PRINT 'LOADING DATA FOR silver.erp_loc_a101'
		PRINT '--------------------------------'

		SET @start_time = GETDATE();

		TRUNCATE TABLE silver.erp_loc_a101;
		insert into silver.erp_loc_a101
		(
		cid,
		cntry
		)
		select 
		replace(cid,'-','') as cid,
		CASE 
			WHEN cntry = '' or cntry is null THEN 'Unknown'
			WHEN UPPER(cntry) in ('US','UNITED STATES','USA') THEN 'United States'
			WHEN UPPER(cntry) in ('DE','GERMANY') THEN 'Germany'
			ELSE cntry
		END cntry
		from bronze.erp_loc_a101
		
		;

		SET @end_time = GETDATE();
		PRINT 'LOADED DATA, PROCESS TOOK : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds'

		PRINT '--------------------------------'
		PRINT 'LOADING DATA FOR silver.erp_px_cat_g1v2'
		PRINT '--------------------------------'

		SET @start_time = GETDATE();

		TRUNCATE TABLE silver.erp_px_cat_g1v2;

		insert into silver.erp_px_cat_g1v2
		(
		id,
		cat,
		subcat,
		maintainence 
		)
		select id,
		cat,
		subcat,
		maintainence 
		from bronze.erp_px_cat_g1v2
		;
	
	SET @end_time = GETDATE();
	PRINT 'LOADED DATA, PROCESS TOOK : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + ' seconds'

	PRINT '--------------------------------------------------'
	SET @batchend_time = GETDATE();
	PRINT 'BATCH COMPLETED IN: ' + CAST(DATEDIFF(second,@batchstart_time,@batchend_time) as VARCHAR) + ' seconds' 
	END TRY 

	BEGIN CATCH
		PRINT '==============================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '==============================================='
	END CATCH
	PRINT 'PROCESS COMPLETED IN : ' + CAST(DATEDIFF(second,@batchstart_time,@batchend_time) AS VARCHAR) + ' seconds'
END
;



