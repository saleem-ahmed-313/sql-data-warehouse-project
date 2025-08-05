/*
=============================================================
  Create Database and Schemas
=============================================================
Script Purpose:
    1. Checks if 'DataWarehouse' database exists.
    2. Drops it if present (WARNING: Data loss risk!).
    3. Recreates the 'DataWarehouse' database.
    4. Creates three schemas: bronze, silver, gold.

WARNING:
    Running this script will DROP the existing 'DataWarehouse' database 
    and all its contents permanently.
    Ensure you have backups before executing this script.
=============================================================
*/

-- Switch to master to ensure safe DB drop/create
USE master;
GO

-------------------------------------------------------------
-- DROP EXISTING DATABASE IF IT EXISTS
-------------------------------------------------------------
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    PRINT 'Database "DataWarehouse" exists. Dropping it now...';
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
    PRINT 'Database "DataWarehouse" dropped successfully.';
END;
GO

-------------------------------------------------------------
-- CREATE NEW DATABASE
-------------------------------------------------------------
PRINT 'Creating new database "DataWarehouse"...';
CREATE DATABASE DataWarehouse;
GO

-- Switch to the newly created database
USE DataWarehouse;
GO

-------------------------------------------------------------
-- CREATE SCHEMAS: bronze, silver, gold
-------------------------------------------------------------
PRINT 'Creating schemas...';

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

PRINT 'Schemas created successfully.';
