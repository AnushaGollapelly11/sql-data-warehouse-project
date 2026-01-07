
/* 
================================================================

Create Database and Schemas 

================================================================

📌 Script Purpose: 

This script drops and recreates the DataWarehouse database and creates the core schemas (bronze, silver, and gold). 
It ensures a clean database setup for organizing data according to the Medallion Architecture.

⚠️ Warning

This script will permanently delete the existing DataWarehouse database if it already exists.
All data inside the database will be lost during execution.Proceed with caution and take a backup if the database contains important data.

*/

USE master;

--- Drop and recreate the "DataWarehouse' database 
IF EXISTS ( SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
END; 

--- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

--- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;

