/*
================================================================================================
 Script Name: init_database.sql
================================================================================================
 Purpose:
 --------
 Initializes the Data Warehouse environment by:
   1. Dropping the existing 'DataWarehouse' database (if it exists)
   2. Recreating the 'DataWarehouse' database
   3. Creating core schemas used in the Medallion Architecture:
        - bronze : Raw, source-aligned data
        - silver : Cleaned and conformed data
        - gold   : Business-ready analytics layer

 WARNING:
 --------
 This script is DESTRUCTIVE.
 If the 'DataWarehouse' database already exists, it will be DROPPED and RECREATED.
 ALL existing data will be permanently lost.

 Recommended Usage:
 ------------------
 - Run only during initial environment setup
 - Run in local/dev environments
 - Do NOT run in production without backups and approvals

 Execution Context:
 ------------------
 - Must be executed by a user with database creation privileges
 - Uses the 'master' database context for DROP/CREATE operations
================================================================================================
*/

-- Switch context to master database (required for DROP/CREATE DATABASE)
USE master;
GO

/* -------------------------------------------------------------
   Drop existing DataWarehouse database (if present)
   ------------------------------------------------------------- */
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    -- Force disconnect all users and rollback open transactions
    ALTER DATABASE DataWarehouse 
        SET SINGLE_USER 
        WITH ROLLBACK IMMEDIATE;

    DROP DATABASE DataWarehouse;
END;
GO

/* -------------------------------------------------------------
   Create DataWarehouse database
   ------------------------------------------------------------- */
CREATE DATABASE DataWarehouse;
GO

-- Switch context to newly created database
USE DataWarehouse;
GO

/* -------------------------------------------------------------
   Create Schemas (Medallion Architecture)
   ------------------------------------------------------------- */

-- Bronze schema: Raw data ingestion layer
CREATE SCHEMA bronze;
GO

-- Silver schema: Cleaned, standardized, and conformed data
CREATE SCHEMA silver;
GO

-- Gold schema: Aggregated, business-ready analytics layer
CREATE SCHEMA gold;
GO
