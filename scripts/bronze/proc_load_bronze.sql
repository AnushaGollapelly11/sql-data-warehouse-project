/* ============================================================
   Stored Procedure Name:
   bronze.load_bronze

   Source:
   - CRM CSV files (cust_info, prd_info, sales_details)
   - ERP CSV files (CUST_AZ12, LOC_A101, PX_CAT_G1V2)
   - Files are loaded from local file system paths using BULK INSERT

   Script Purpose:
   - Loads raw CRM and ERP source data into Bronze layer tables.
   - Ensures idempotent loads by truncating tables before inserting data.
   - Uses BULK INSERT with TABLOCK to improve performance during data ingestion.
   - Captures load duration and logs progress for monitoring and debugging.

   Parameters / Return Values:
   - This stored procedure does NOT accept any input parameters.
   - It does NOT return any values.
   - Execution details are logged using PRINT statements.

   Usage Example:
   EXEC bronze.load_bronze;
   ============================================================ */

EXEC bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
	SET @batch_start_time = GETDATE();
		PRINT '====================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '====================================================';
	--- if we run again insert command then the table data will load twice to avoid this frist we truncate the table(making table empty) and then load the data 
		PRINT '------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Data into : bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info 
		FROM 'C:\Users\archa\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK --- locking entire table during loading data to imrpove performance When a table is locked, SQL Server restricts other sessions from reading or writing data in that table until the lock is released.
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +  ' seconds';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Data into : bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info 
		FROM 'C:\Users\archa\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK --- locking entire table during loading data to imrpove performance When a table is locked, SQL Server restricts other sessions from reading or writing data in that table until the lock is released.
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +  ' seconds';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data into : bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\archa\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK --- locking entire table during loading data to imrpove performance When a table is locked, SQL Server restricts other sessions from reading or writing data in that table until the lock is released.
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +  ' seconds';

		PRINT '------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data into : bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12 
		FROM 'C:\Users\archa\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK --- locking entire table during loading data to imrpove performance When a table is locked, SQL Server restricts other sessions from reading or writing data in that table until the lock is released.
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +  ' seconds';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data into : bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101 
		FROM 'C:\Users\archa\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK --- locking entire table during loading data to imrpove performance When a table is locked, SQL Server restricts other sessions from reading or writing data in that table until the lock is released.
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +  ' seconds';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data into : bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2 
		FROM 'C:\Users\archa\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK --- locking entire table during loading data to imrpove performance When a table is locked, SQL Server restricts other sessions from reading or writing data in that table until the lock is released.
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) +  ' seconds';
		PRINT '>>-----------';

		SET @batch_end_time = GETDATE();
		PRINT '========================================================';
		PRINT 'Loading Bronze Layer is COmpleted';
		PRINT ' - Total  Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) +  ' seconds';
		PRINT '========================================================';
	END TRY
	BEGIN CATCH 
		PRINT '=============================================================='
		PRINT ' ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT '=============================================================='

	END CATCH
END

