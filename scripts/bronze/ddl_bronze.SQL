/*
================================================================================================
 DDL Script: Bronze Layer Table Creation
================================================================================================
 Purpose:
 --------
 This script creates all tables for the Bronze layer in the data warehouse.
 The Bronze layer stores raw, source-aligned data with minimal transformation.

 Key Characteristics of Bronze Layer:
 ------------------------------------
 - Tables closely mirror the source system structure
 - No business logic or transformations are applied
 - Data types are kept flexible to accommodate raw ingestion
 - Tables are fully recreated when this script is run

 WARNING:
 --------
 Running this script will DROP and RECREATE all Bronze tables.
 Any existing data in the Bronze layer will be permanently lost.

 Intended Usage:
 ---------------
 - Run during initial environment setup
 - Run when source structure changes
 - Should be followed by Bronze data load procedures

================================================================================================
*/

 /* =============================================================
    CRM SOURCE TABLES
    ============================================================= */

-- ----------------------------------------------------------------
-- Table: bronze.crm_cust_info
-- Description:
--   Stores raw customer master data from the CRM source system.
--   One record per customer.
-- ----------------------------------------------------------------
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,            -- Surrogate or system-generated customer ID
    cst_key             NVARCHAR(50),    -- Business/customer key from source system
    cst_firstname       NVARCHAR(50),    -- Customer first name
    cst_lastname        NVARCHAR(50),    -- Customer last name
    cst_material_status NVARCHAR(50),    -- Marital status (raw value from source)
    cst_gndr            NVARCHAR(50),    -- Gender (raw, not standardized)
    cst_create_date     DATE             -- Customer creation date in source system
);


-- ----------------------------------------------------------------
-- Table: bronze.crm_prd_info
-- Description:
--   Stores raw product master data from the CRM system.
--   Contains product attributes and lifecycle dates.
-- ----------------------------------------------------------------
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info (
    prd_id        INT,            -- Internal product ID
    prd_key       NVARCHAR(50),    -- Business product key
    prd_nm        NVARCHAR(50),    -- Product name
    prd_cost      INT,             -- Product cost (raw, no currency normalization)
    prd_line      NVARCHAR(5),     -- Product line/category code
    prd_start_dt  DATE,            -- Product availability start date
    prd_end_dt    DATE             -- Product availability end date
);


-- ----------------------------------------------------------------
-- Table: bronze.crm_sales_details
-- Description:
--   Stores transactional sales data from the CRM system.
--   Represents individual sales order line items.
-- ----------------------------------------------------------------
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num   NVARCHAR(50),    -- Sales order number
    sls_prd_key   NVARCHAR(50),    -- Product business key
    sls_cust_id   INT,             -- Customer ID
    sls_order_dt  INT,             -- Order date (raw format from source)
    sls_ship_dt   INT,             -- Shipment date (raw format from source)
    sls_due_dt    INT,             -- Payment due date (raw format from source)
    sls_sales     INT,             -- Total sales amount (raw)
    sls_quantity  INT,             -- Quantity sold
    sls_price     INT              -- Unit price
);


 /* =============================================================
    ERP SOURCE TABLES
    ============================================================= */

-- ----------------------------------------------------------------
-- Table: bronze.erp_cust_az12
-- Description:
--   Stores customer demographic data sourced from ERP system AZ12.
-- ----------------------------------------------------------------
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;

CREATE TABLE bronze.erp_cust_az12 (
    cid   NVARCHAR(50),    -- Customer identifier from ERP system
    bdate DATE,            -- Birth date
    gen   NVARCHAR(50)     -- Gender (raw ERP value)
);


-- ----------------------------------------------------------------
-- Table: bronze.erp_px_cat_g1v2
-- Description:
--   Stores product category and hierarchy information from ERP.
-- ----------------------------------------------------------------
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),    -- Product identifier
    cat          NVARCHAR(50),    -- Product category
    subcat       NVARCHAR(50),    -- Product sub-category
    maintenance  NVARCHAR(50)     -- Maintenance classification or flag
);


-- ----------------------------------------------------------------
-- Table: bronze.erp_loc_a101
-- Description:
--   Stores customer location and country information from ERP system.
-- ----------------------------------------------------------------
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;

CREATE TABLE bronze.erp_loc_a101 (
    cid   NVARCHAR(50),    -- Customer identifier
    cntry NVARCHAR(50)     -- Country name or code
);
