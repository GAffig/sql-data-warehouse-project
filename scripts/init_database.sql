/*
=================================================
Create Database (db) and Schemas
=================================================

Script Purpose:
  This script creates a new db name 'DataWarehouse' after checking if it already exists.
  If the db exists, it is dropped and recreated. Additionally, the script sets up three schemas
  within the db: 'bronze', 'silver', 'gold'

WARNING:
  Running this script will drop the entire 'DataWarehouse' db if it exists.
  All data in the db will be permanently deleted. Proceed with caution and ensure
  you have the proper backups running the script.
*/


USE master;
GO
-- Drop and recreate the 'DataWarehouse' database
IF EXISTS ( SELECT 1 FROM sys.databases WHERE name = 'Datawarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREARE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
