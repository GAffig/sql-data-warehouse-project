/*
================================================================================================
 Stored Procedure: bronze.load_bronze
================================================================================================
 Purpose:
 --------
 Loads raw CSV source data into Bronze layer tables using full refresh logic.
 Includes row count logging and load duration tracking for monitoring purposes.

 This is part of a Medallion Architecture (Bronze layer).
================================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    /* -------------------------------------------------------------
       Variable Declarations
       ------------------------------------------------------------- */
    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME,
        @row_count INT;              -- Stores number of rows loaded per table

    BEGIN TRY
        /* -------------------------------------------------------------
           Batch Start
           ------------------------------------------------------------- */
        SET @batch_start_time = GETDATE();

        PRINT '=====================================================================================';
        PRINT '                             Loading Bronze Layer';
        PRINT '=====================================================================================';

        /* =============================================================
           CRM TABLES
           ============================================================= */
        PRINT '------------------------------------------------------------------------------';
        PRINT '                        Loading CRM Tables';
        PRINT '------------------------------------------------------------------------------';

        /* ------------------------------
           bronze.crm_cust_info
           ------------------------------ */
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting Data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\gaffi\Downloads\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        -- Capture row count
        SET @row_count = @@ROWCOUNT;

        SET @end_time = GETDATE();
        PRINT '>> Rows Loaded: ' + CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------';

        /* ------------------------------
           bronze.crm_prd_info
           ------------------------------ */
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting Data Into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\gaffi\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @row_count = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '>> Rows Loaded: ' + CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------';

        /* ------------------------------
           bronze.crm_sales_details
           ------------------------------ */
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting Data Into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\gaffi\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @row_count = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '>> Rows Loaded: ' + CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------';

        /* =============================================================
           ERP TABLES
           ============================================================= */
        PRINT '------------------------------------------------------------------------------';
        PRINT '                        Loading ERP Tables';
        PRINT '------------------------------------------------------------------------------';

        /* ------------------------------
           bronze.erp_cust_az12
           ------------------------------ */
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\gaffi\Downloads\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @row_count = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '>> Rows Loaded: ' + CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------';

        /* ------------------------------
           bronze.erp_loc_a101
           ------------------------------ */
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\gaffi\Downloads\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @row_count = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '>> Rows Loaded: ' + CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------';

        /* ------------------------------
           bronze.erp_px_cat_g1v2
           ------------------------------ */
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\gaffi\Downloads\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @row_count = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '>> Rows Loaded: ' + CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------';

        /* -------------------------------------------------------------
           Batch End
           ------------------------------------------------------------- */
        SET @batch_end_time = GETDATE();

        PRINT '==================================================================';
        PRINT ' Loading Bronze Layer Completed Successfully';
        PRINT ' Total Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR)
              + ' seconds';
        PRINT '==================================================================';

    END TRY
    BEGIN CATCH
        PRINT '======================================================================';
        PRINT ' ERROR OCCURRED DURING BRONZE LOAD';
        PRINT ' Error Message: ' + ERROR_MESSAGE();
        PRINT ' Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT ' Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '======================================================================';
    END CATCH
END;
